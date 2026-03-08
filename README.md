# OpenSanctions Dashboard

This project aims to create a dashboard that enables a visual analysis of 
inter-countries sanctions from a business perspective 
using the [OpenSanctions Default](https://www.opensanctions.org/datasets/default/) dataset from [OpenSanctions](https://www.opensanctions.org/datasets/default/). 

Additionally, the [7+ Million Company Dataset](https://www.kaggle.com/datasets/peopledatalabssf/free-7-million-company-dataset) 
from [People Data Labs](https://www.peopledatalabs.com/) is joined in order to include affected industries. 

The backend now includes a cleaner analytical layer:
- `sanction_edges`: normalized source-country -> target-country sanctions facts
- `country_sanction_timeseries`: monthly/quarterly aggregation for heatmap and timeline use-cases
- `country_spi_timeseries`: monthly SPI-ready country metrics (total, orgs, persons, growth, diversity)

## Features

### Sanctions By Country
![SanctionsByCountry.png](img/SanctionsByCountry.png)

### Entity Search
![EntitySearch.png](img/EntitySearch.png)

### Network Analysis
![NetworkAnalysis.png](img/NetworkAnalysis.png)

## Installation

* Install [PostgreSQL](https://www.postgresql.org/) & [Python](https://www.python.org/)
* Download Files into a `data` folder (replace YYYYMMDD with the current data)
  * https://data.opensanctions.org/datasets/YYYYMMDD/default/index.json
  * https://data.opensanctions.org/datasets/YYYYMMDD/default/entities.ftm.json
  * https://www.kaggle.com/datasets/peopledatalabssf/free-7-million-company-dataset
* create virtual env and install the requirements
  ```bash 
    python -m venv .venv
    source .venv/bin/activate
    pip3 install -r requirements.txt
  ```
* Create database and insert the data (+transformations)
  * Create database 
    ```bash 
    sudo -u postgres psql 
    ``` 
    In the PostgreSQL editor insert the following SQL code:
    ```SQL
    CREATE DATABASE sanctions; 
    ALTER DATABASE sanctions OWNER TO sanctions;
    CREATE USER sanctions WITH ENCRYPTED PASSWORD 'sanctions';
    ALTER DATABASE sanctions OWNER TO sanctions;
    GRANT ALL PRIVILEGES ON DATABASE sanctions to sanctions;
    GRANT ALL ON SCHEMA sanctions.public TO sanctions;
    ```
  * Create schema
    ```bash
    python ./util/DB.py sql/schema.sql
    ```
  * Insert OpenSanctions entries, datasets, and companies in the database
    ```bash
    # Insert OpenSanctions datasets and entities
    python ./util/ParseOpenSanctionsData.py download_datasets ./data/index.json
    python ./util/ParseOpenSanctionsData.py write_entities ./data/entities.ftm.json
  
    # Insert company data
    python ./util/ParserCompanySetData.py parser_company_set_data ./data/companies_sorted.csv
    ```
  * Insert country metadata and build analytical tables/views
    ```bash
    python ./util/DB.py ./sql/countries.sql
    python ./util/DB.py ./sql/index_and_joins.sql
    ```
  * If your database already exists from an older version, run:
    ```bash
    python ./util/DB.py ./sql/migration_add_analytics.sql
    python ./util/DB.py ./sql/index_and_joins.sql
    ```
* Start the Dashboard:
  ```bash
  python ./sanctions_dashboard/dashboard.py
  ```

## Export Analytical Outputs

Use the export script to create reproducible filtered analytical outputs for heatmaps/SPI:

```bash
# Country sanctions timeseries (CSV)
python ./util/ExportAnalytics.py timeseries --bucket month --format csv --output ./data/country_timeseries.csv

# SPI timeseries for selected targets and schemas (JSON)
python ./util/ExportAnalytics.py spi --bucket month --format json --output ./data/spi_timeseries.json --target-countries ru,ir --schemas Company,Organization,Person
```

Supported filters:
- `--start-date`, `--end-date`
- `--schemas`
- `--industries`
- `--datasets`
- `--source-countries`
- `--target-countries`
  
## Disclaimer
This dashboard was created by TU Vienna student [Nicolas Bschor](https://github.com/HackerBschor) in collaboration 
with WU Vienna Professor [Dr. Jakob Müllner](https://www.wu.ac.at/iib/iib/faculty/muellner/) during the 
'Interdisciplinary Project in Data Science' course for academic research of sanctions.

It may only be used for **non-commercial purposes**!

The data used in this dashboard is sourced from [OpenSanctions](https://www.opensanctions.org/) (Sanctions Information) 
and [People Data Labs](https://www.peopledatalabs.com/) (Company Industries).


We only applied various transformation techniques to allow the analysis without changing the information. 
Therefore, we cannot ensure completeness, correctness, or if the data is up to date.
Users are advised to verify information independently, and the developers assume no responsibility for the 
consequences of its use. Use the dashboard at your own risk.
