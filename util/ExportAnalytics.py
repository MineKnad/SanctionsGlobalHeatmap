import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, create_engine


ORG_SCHEMAS: tuple[str, ...] = ("Company", "Organization", "LegalEntity")


def _split_csv_arg(raw: str | None, lower: bool = False) -> list[str] | None:
    if raw is None or raw.strip() == "":
        return None

    values = [v.strip() for v in raw.split(",")]
    values = [v.lower() if lower else v for v in values if v != ""]
    return values if len(values) > 0 else None


def _build_where_clause(args: argparse.Namespace) -> tuple[str, dict]:
    conditions: list[str] = []
    params: dict = {}

    if args.start_date is not None:
        conditions.append("first_seen >= %(start_date)s")
        params["start_date"] = args.start_date

    if args.end_date is not None:
        conditions.append("first_seen <= %(end_date)s")
        params["end_date"] = args.end_date

    schemas = _split_csv_arg(args.schemas)
    if schemas is not None:
        conditions.append("schema = ANY(%(schemas)s)")
        params["schemas"] = schemas

    industries = _split_csv_arg(args.industries)
    if industries is not None:
        conditions.append("industry = ANY(%(industries)s)")
        params["industries"] = industries

    datasets = _split_csv_arg(args.datasets)
    if datasets is not None:
        conditions.append("dataset_name = ANY(%(datasets)s)")
        params["datasets"] = datasets

    source_countries = _split_csv_arg(args.source_countries, lower=True)
    if source_countries is not None:
        conditions.append("source_country = ANY(%(source_countries)s)")
        params["source_countries"] = source_countries

    target_countries = _split_csv_arg(args.target_countries, lower=True)
    if target_countries is not None:
        conditions.append("target_country = ANY(%(target_countries)s)")
        params["target_countries"] = target_countries

    where_sql = " AND ".join(conditions)
    return ("TRUE" if where_sql == "" else where_sql), params


def build_country_timeseries(args: argparse.Namespace, engine: Engine) -> pd.DataFrame:
    bucket_expr = "date_trunc('month', first_seen)::date" if args.bucket == "month" \
        else "date_trunc('quarter', first_seen)::date"
    where_sql, params = _build_where_clause(args)

    sql = f"""
        SELECT
            {bucket_expr} AS time_bucket,
            source_country,
            target_country,
            schema,
            industry,
            dataset_name,
            COUNT(DISTINCT entity_id) AS entity_count
        FROM sanction_edges
        WHERE {where_sql}
        GROUP BY 1, 2, 3, 4, 5, 6
        ORDER BY 1, 2, 3, 4, 5, 6
    """
    return pd.read_sql(sql, con=engine, params=params)


def build_spi_timeseries(args: argparse.Namespace, engine: Engine) -> pd.DataFrame:
    bucket_expr = "date_trunc('month', first_seen)::date" if args.bucket == "month" \
        else "date_trunc('quarter', first_seen)::date"
    where_sql, params = _build_where_clause(args)
    params["org_schemas"] = list(ORG_SCHEMAS)

    sql = f"""
        WITH filtered AS (
            SELECT *
            FROM sanction_edges
            WHERE {where_sql}
        ),
        aggregates AS (
            SELECT
                {bucket_expr} AS time_bucket,
                target_country AS country,
                COUNT(DISTINCT entity_id) AS total_entities,
                COUNT(DISTINCT entity_id) FILTER (WHERE schema = ANY(%(org_schemas)s)) AS org_entities,
                COUNT(DISTINCT entity_id) FILTER (WHERE schema = 'Person') AS person_entities,
                COUNT(DISTINCT source_country) AS source_country_diversity,
                COUNT(DISTINCT dataset_name) AS authority_diversity
            FROM filtered
            GROUP BY 1, 2
        ),
        growth AS (
            SELECT
                time_bucket,
                country,
                total_entities,
                org_entities,
                person_entities,
                GREATEST(
                    total_entities - LAG(total_entities, 1, total_entities)
                        OVER (PARTITION BY country ORDER BY time_bucket),
                    0
                ) AS recent_growth,
                source_country_diversity,
                authority_diversity
            FROM aggregates
        )
        SELECT
            g.time_bucket,
            g.country,
            COALESCE(NULLIF(c.name, ''), NULLIF(c.description, ''), g.country) AS country_name,
            g.total_entities AS spi_total,
            g.org_entities AS spi_orgs,
            g.person_entities AS spi_persons,
            g.recent_growth AS spi_recent_growth,
            g.source_country_diversity,
            g.authority_diversity,
            ROUND(
                g.total_entities::numeric +
                0.5 * g.source_country_diversity::numeric +
                0.25 * g.authority_diversity::numeric +
                0.5 * g.recent_growth::numeric,
                3
            ) AS spi
        FROM growth g
        LEFT JOIN countries c ON c.alpha_2 = g.country
        ORDER BY g.time_bucket, g.country
    """
    return pd.read_sql(sql, con=engine, params=params)


def export_dataframe(df: pd.DataFrame, output: Path, export_format: str) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    if export_format == "json":
        df.to_json(output, orient="records", force_ascii=False, indent=2, date_format="iso")
        return

    df.to_csv(output, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export sanctions analytical outputs (timeseries/SPI) from sanction_edges."
    )
    parser.add_argument("mode", choices=["timeseries", "spi"], help="Analytical output to export.")
    parser.add_argument("--bucket", choices=["month", "quarter"], default="month", help="Time aggregation bucket.")
    parser.add_argument("--output", required=True, help="Output file path.")
    parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output serialization format.")
    parser.add_argument("--start-date", dest="start_date", default=None, help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", dest="end_date", default=None, help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument("--schemas", default=None, help="Comma-separated schema list.")
    parser.add_argument("--industries", default=None, help="Comma-separated industry list.")
    parser.add_argument("--datasets", default=None, help="Comma-separated dataset names/authorities.")
    parser.add_argument("--source-countries", dest="source_countries", default=None,
                        help="Comma-separated source country alpha-2 codes.")
    parser.add_argument("--target-countries", dest="target_countries", default=None,
                        help="Comma-separated target country alpha-2 codes.")
    parser.add_argument("--db-url", dest="db_url",
                        default="postgresql+psycopg2://sanctions:sanctions@localhost:5432/sanctions",
                        help="SQLAlchemy database URL.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine: Engine = create_engine(args.db_url)

    if args.mode == "timeseries":
        df = build_country_timeseries(args, engine)
    else:
        df = build_spi_timeseries(args, engine)

    export_dataframe(df, Path(args.output), args.format)
    print(f"Exported {len(df)} rows to {args.output}")


if __name__ == "__main__":
    main()
