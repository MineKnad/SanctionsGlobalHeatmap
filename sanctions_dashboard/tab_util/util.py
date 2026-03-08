import pandas as pd
from sqlalchemy import Engine
from pathlib import Path
from typing import Callable


def df_to_excel(df: pd.DataFrame, sheet_name: str) -> callable:
    def to_xlsx(bytes_io):
        xslx_writer: pd.ExcelWriter = pd.ExcelWriter(bytes_io, engine="xlsxwriter")
        pd.DataFrame(df).to_excel(xslx_writer, index=False, sheet_name=sheet_name)
        xslx_writer.sheets[sheet_name].autofit()
        xslx_writer.close()

    return to_xlsx


def _read_lines(file_path: str | Path | None) -> list[str]:
    if file_path is None:
        return []

    path = Path(file_path)
    if not path.exists():
        return []

    with open(path, "r", encoding="utf-8") as fd:
        return [line.strip() for line in fd.read().splitlines() if line.strip() != ""]


def _fetch_options(engine: Engine, sql: str, value_col: str = "value", label_col: str = "label",
                   transform_label: Callable[[str], str] | None = None) -> list[dict]:
    try:
        df: pd.DataFrame = pd.read_sql(sql, con=engine)
    except Exception:
        return []

    options: list[dict] = []
    for row in df.to_dict("records"):
        value: str = row[value_col]
        label: str = row[label_col] if label_col in row else value
        if value is None or str(value).strip() == "":
            continue

        if transform_label is not None:
            label = transform_label(str(label))

        options.append({"label": str(label), "value": str(value)})

    return options


def create_country_list(engine: Engine, col: str | None = None) -> list[dict]:
    if col is not None:
        sql: str = f"""SELECT DISTINCT ec.{col} AS value,
                          COALESCE(NULLIF(c.name, ''), NULLIF(c.description, ''), ec.{col}) AS label
                       FROM entities_countries ec
                       LEFT JOIN countries c ON ec.{col} = c.alpha_2
                       WHERE ec.{col} IS NOT NULL AND ec.{col} <> ''
                       ORDER BY 2, 1"""
    else:
        sql: str = """SELECT alpha_2 AS value,
                         COALESCE(NULLIF(c.name, ''), NULLIF(c.description, ''), alpha_2) AS label
                      FROM (
                        SELECT source_country AS alpha_2 FROM entities_countries
                        UNION
                        SELECT target_country AS alpha_2 FROM entities_countries
                      ) a
                      LEFT JOIN countries c USING (alpha_2)
                      WHERE alpha_2 IS NOT NULL AND alpha_2 <> ''
                      ORDER BY 2, 1"""

    return _fetch_options(engine, sql)


def create_schema_list(engine: Engine, fallback_file: str | Path | None = None) -> list[dict]:
    sql: str = """SELECT DISTINCT schema AS value
                  FROM entities_countries
                  WHERE schema IS NOT NULL AND schema <> ''
                  ORDER BY schema"""
    options = _fetch_options(engine, sql, value_col="value", label_col="value")

    if len(options) > 0:
        return options

    return [{"label": v, "value": v} for v in _read_lines(fallback_file)]


def create_industry_list(engine: Engine, fallback_file: str | Path | None = None) -> list[dict]:
    def _format_label(v: str) -> str:
        return " ".join([x.capitalize() for x in v.replace("/", " / ").split(" ")])

    sql: str = """SELECT DISTINCT industry AS value
                  FROM entities_countries
                  WHERE industry IS NOT NULL AND industry <> ''
                  ORDER BY industry"""
    options = _fetch_options(engine, sql, value_col="value", label_col="value", transform_label=_format_label)

    if len(options) > 0:
        return options

    return [{"label": _format_label(v), "value": v} for v in _read_lines(fallback_file)]
