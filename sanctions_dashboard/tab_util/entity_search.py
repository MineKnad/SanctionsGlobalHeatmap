import pandas as pd
from sqlalchemy import Engine


def search_entity(schema: str, query: str, country: str, engine: Engine) -> pd.DataFrame:
    if query is None or len(query.strip()) == 0:
        return pd.DataFrame()

    country_join: str = ""
    restriction: list[str] = ["LOWER(e.caption) LIKE concat('%%', LOWER(%(query)s), '%%')"]

    if schema is not None and schema.strip() != "":
        restriction.append("e.schema = %(schema)s")

    if country is not None and country.strip() != "":
        country_join = """JOIN (
                            SELECT DISTINCT id FROM entities_countries
                            WHERE source_country = %(country)s OR target_country = %(country)s
                          ) ec_filter USING (id)"""

    sql: str = f"""SELECT
        caption,
        STRING_AGG(DISTINCT country_descr, ', ') AS country_descr,
        e.first_seen,
        e.last_seen,
        e.last_change,
        STRING_AGG(DISTINCT d.title, '\n') AS datasets
        FROM (
            SELECT DISTINCT id, caption, first_seen, last_seen, last_change, json_array_elements_text(datasets) AS name
            FROM entities e
            {country_join}
            WHERE {" AND ".join(restriction)}
        ) e
        LEFT JOIN (SELECT id, target_country FROM entities_countries) ec USING (id)
        LEFT JOIN (
            SELECT alpha_2 AS target_country,
                   COALESCE(NULLIF(name, ''), NULLIF(description, ''), alpha_2) AS country_descr
            FROM countries
        ) c USING (target_country)
        JOIN datasets d USING (name)
        GROUP BY 1,3,4,5"""

    df: pd.DataFrame = pd.read_sql(
        sql,
        params={"schema": schema, "query": query, "country": country},
        con=engine
    )

    df["first_seen"] = pd.to_datetime(df["first_seen"]).dt.date
    df["last_seen"] = pd.to_datetime(df["last_seen"]).dt.date
    df["last_change"] = pd.to_datetime(df["last_change"]).dt.date

    return df.rename(columns={
        "caption": "Title",
        "country_descr": "Country",
        "datasets": "Datasets",
        "first_seen": "First Seen",
        "last_seen": "Last Seen",
        "last_change": "Last Change"
    })