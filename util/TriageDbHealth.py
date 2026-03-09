import argparse
from collections import OrderedDict

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


DEFAULT_DB_URL = "postgresql+psycopg2://sanctions:sanctions@localhost:5432/sanctions"


QUERIES = OrderedDict([
    ("entities_total", "SELECT COUNT(*) FROM entities"),
    ("entities_company", "SELECT COUNT(*) FROM entities WHERE schema = 'Company'"),
    ("entities_with_industry", "SELECT COUNT(*) FROM entities WHERE industry IS NOT NULL AND industry <> ''"),
    ("datasets_total", "SELECT COUNT(*) FROM datasets"),
    ("datasets_external", "SELECT COUNT(*) FROM datasets WHERE type = 'external'"),
    ("companies_total", "SELECT COUNT(*) FROM companies"),
    ("companies_with_industry", "SELECT COUNT(*) FROM companies WHERE industry IS NOT NULL AND industry <> ''"),
    ("sanction_edges_total", "SELECT COUNT(*) FROM sanction_edges"),
    ("sanction_edges_distinct_entity", "SELECT COUNT(DISTINCT entity_id) FROM sanction_edges"),
    ("sanction_edges_missing_source", "SELECT COUNT(*) FROM sanction_edges WHERE source_country IS NULL OR source_country = ''"),
    ("sanction_edges_missing_target", "SELECT COUNT(*) FROM sanction_edges WHERE target_country IS NULL OR target_country = ''"),
    ("entities_countries_total", "SELECT COUNT(*) FROM entities_countries"),
    ("entities_countries_distinct_id", "SELECT COUNT(DISTINCT id) FROM entities_countries"),
    ("network_country_pairs", """
        SELECT COUNT(*) FROM (
            SELECT source_country, target_country
            FROM entities_countries
            WHERE source_country <> target_country
            GROUP BY 1, 2
        ) q
    """),
    ("entities_countries_with_industry", """
        SELECT COUNT(*)
        FROM entities_countries
        WHERE industry IS NOT NULL AND industry <> ''
    """),
])


def run_scalar(engine: Engine, sql: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar_one()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run repeatable DB triage checks for sanctions dashboard tables.")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL, help="SQLAlchemy database URL.")
    args = parser.parse_args()

    engine = create_engine(args.db_url)
    results: dict[str, str] = {}

    for key, sql in QUERIES.items():
        try:
            results[key] = str(run_scalar(engine, sql))
        except Exception as exc:
            results[key] = f"ERROR: {exc}"

    print("=== Sanctions Dashboard DB Triage ===")
    for key, value in results.items():
        print(f"{key}: {value}")

    try:
        company_entities = int(results["entities_company"])
        company_with_industry = run_scalar(
            engine,
            "SELECT COUNT(*) FROM entities WHERE schema = 'Company' AND industry IS NOT NULL AND industry <> ''"
        )
        rate = (company_with_industry / company_entities) if company_entities > 0 else 0.0
        print(f"company_industry_enrichment_rate: {company_with_industry}/{company_entities} ({rate:.2%})")
    except Exception as exc:
        print(f"company_industry_enrichment_rate: ERROR: {exc}")


if __name__ == "__main__":
    main()
