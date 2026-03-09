import argparse

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


DEFAULT_DB_URL = "postgresql+psycopg2://sanctions:sanctions@localhost:5432/sanctions"


OLD_LOGIC_CTE = """
WITH old_logic AS (
    SELECT DISTINCT
        id,
        caption,
        schema,
        json_array_elements_text(COALESCE(properties->'country', properties->'jurisdiction')) AS target_country,
        publisher->>'country' AS source_country,
        first_seen,
        last_seen,
        last_change,
        target,
        industry
    FROM (
        SELECT id, caption, schema, first_seen, last_seen, last_change, target, properties,
               json_array_elements_text(datasets) AS name, industry
        FROM entities
    ) e
    JOIN (SELECT * FROM datasets WHERE type <> 'external') d USING (name)
),
old_filtered AS (
    SELECT *
    FROM old_logic
    WHERE source_country IS NOT NULL
      AND target_country IS NOT NULL
),
new_logic AS (
    SELECT
        id,
        caption,
        schema,
        target_country,
        source_country,
        first_seen,
        last_seen,
        last_change,
        target,
        industry
    FROM entities_countries
)
"""


def run_scalar(engine: Engine, sql: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar_one()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Controlled A/B comparison: old entities_countries transform logic vs current table."
    )
    parser.add_argument("--db-url", default=DEFAULT_DB_URL, help="SQLAlchemy database URL.")
    args = parser.parse_args()

    engine = create_engine(args.db_url)

    stats_sql = OLD_LOGIC_CTE + """
SELECT
    (SELECT COUNT(*) FROM old_filtered) AS old_rows,
    (SELECT COUNT(DISTINCT id) FROM old_filtered) AS old_distinct_ids,
    (SELECT COUNT(*) FROM new_logic) AS new_rows,
    (SELECT COUNT(DISTINCT id) FROM new_logic) AS new_distinct_ids,
    (SELECT COUNT(*) FROM old_filtered WHERE source_country = '' OR target_country = '') AS old_rows_with_empty_country,
    (SELECT COUNT(*) FROM new_logic WHERE source_country = '' OR target_country = '') AS new_rows_with_empty_country,
    (SELECT COUNT(*) FROM (SELECT * FROM old_filtered EXCEPT SELECT * FROM new_logic) a) AS rows_only_old,
    (SELECT COUNT(*) FROM (SELECT * FROM new_logic EXCEPT SELECT * FROM old_filtered) b) AS rows_only_new,
    (SELECT COUNT(*) FROM (
        SELECT DISTINCT id FROM old_filtered
        EXCEPT
        SELECT DISTINCT id FROM new_logic
    ) c) AS ids_only_old,
    (SELECT COUNT(*) FROM (
        SELECT DISTINCT id FROM new_logic
        EXCEPT
        SELECT DISTINCT id FROM old_filtered
    ) d) AS ids_only_new;
"""

    with engine.connect() as conn:
        row = conn.execute(text(stats_sql)).mappings().one()

    print("=== Old vs Current Transform A/B ===")
    for key in row.keys():
        print(f"{key}: {row[key]}")

    if row["rows_only_old"] == 0 and row["rows_only_new"] == 0:
        print("result: current entities_countries is row-equivalent to old logic on this database snapshot.")
    else:
        print("result: detected semantic drift between old and current logic on this database snapshot.")


if __name__ == "__main__":
    main()
