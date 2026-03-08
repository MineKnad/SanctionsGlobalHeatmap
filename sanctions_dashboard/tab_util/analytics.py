import pandas as pd
from sqlalchemy import Engine


def get_country_timeseries(engine: Engine, bucket: str = "month") -> pd.DataFrame:
    bucket_col = "month_bucket" if bucket == "month" else "quarter_bucket"
    sql = f"""SELECT
                {bucket_col} AS time_bucket,
                source_country,
                target_country,
                schema,
                industry,
                dataset_name,
                entity_count
              FROM country_sanction_timeseries
              ORDER BY 1, 2, 3, 4, 5, 6"""
    return pd.read_sql(sql, con=engine)


def get_spi_timeseries(engine: Engine) -> pd.DataFrame:
    sql = """SELECT
                month_bucket AS time_bucket,
                country,
                total_entities,
                org_entities,
                person_entities,
                recent_growth,
                source_country_diversity,
                authority_diversity,
                spi
             FROM country_spi_timeseries
             ORDER BY 1, 2"""
    return pd.read_sql(sql, con=engine)
