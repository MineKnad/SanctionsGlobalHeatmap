/* Non-destructive migration for existing databases */
ALTER TABLE IF EXISTS entities_countries
    ALTER COLUMN schema TYPE varchar(64);

ALTER TABLE IF EXISTS countries
    ALTER COLUMN flag TYPE text,
    ALTER COLUMN name TYPE text,
    ALTER COLUMN description TYPE text;

CREATE TABLE IF NOT EXISTS sanction_edges (
    entity_id VARCHAR(255),
    caption TEXT,
    target_country varchar(8),
    source_country varchar(8),
    schema varchar(64),
    first_seen timestamp,
    last_seen timestamp,
    last_change timestamp,
    target boolean,
    industry text,
    dataset_name text,
    dataset_title text,
    dataset_type text
);

DROP VIEW IF EXISTS country_sanction_timeseries;
DROP VIEW IF EXISTS country_spi_timeseries;
