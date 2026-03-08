/* Join Industries */
UPDATE entities e
SET industry = c.industry
FROM entities e1
JOIN companies c ON LOWER(e1.caption) = LOWER(c.name)
WHERE e.id = e1.id
  AND e.schema = 'Company';

/* Create clean sanctions fact edges */
TRUNCATE TABLE sanction_edges;

INSERT INTO sanction_edges
    (entity_id, caption, schema, target_country, source_country, first_seen, last_seen, last_change, target, industry,
     dataset_name, dataset_title, dataset_type)
SELECT DISTINCT
    ds.id AS entity_id,
    ds.caption,
    ds.schema,
    json_array_elements_text(COALESCE(ds.properties->'country', ds.properties->'jurisdiction')) AS target_country,
    d.publisher->>'country' AS source_country,
    ds.first_seen,
    ds.last_seen,
    ds.last_change,
    ds.target,
    ds.industry,
    ds.name AS dataset_name,
    d.title AS dataset_title,
    d.type AS dataset_type
FROM (
    SELECT id, caption, schema, first_seen, last_seen, last_change, target, properties,
           json_array_elements_text(datasets) AS name, industry
    FROM entities
) ds
JOIN datasets d ON d.name = ds.name
WHERE d.type <> 'external'
  AND d.publisher->>'country' IS NOT NULL
  AND d.publisher->>'country' <> '';

DELETE FROM sanction_edges
WHERE source_country IS NULL OR source_country = ''
   OR target_country IS NULL OR target_country = '';

/* Compatibility table kept for current dashboard */
TRUNCATE TABLE entities_countries;

INSERT INTO entities_countries
    (id, caption, schema, target_country, source_country, first_seen, last_seen, last_change, target, industry)
SELECT DISTINCT
    entity_id AS id,
    caption,
    schema,
    target_country,
    source_country,
    first_seen,
    last_seen,
    last_change,
    target,
    industry
FROM sanction_edges;

/* Analytical views for heatmap/SPI pipeline */
DROP VIEW IF EXISTS country_sanction_timeseries;
CREATE VIEW country_sanction_timeseries AS
SELECT
    date_trunc('month', first_seen)::date AS month_bucket,
    date_trunc('quarter', first_seen)::date AS quarter_bucket,
    source_country,
    target_country,
    schema,
    industry,
    dataset_name,
    COUNT(DISTINCT entity_id) AS entity_count
FROM sanction_edges
GROUP BY 1, 2, 3, 4, 5, 6, 7;

DROP VIEW IF EXISTS country_spi_timeseries;
CREATE VIEW country_spi_timeseries AS
WITH monthly AS (
    SELECT
        date_trunc('month', first_seen)::date AS month_bucket,
        target_country AS country,
        COUNT(DISTINCT entity_id) AS total_entities,
        COUNT(DISTINCT entity_id) FILTER (WHERE schema IN ('Company', 'Organization', 'LegalEntity')) AS org_entities,
        COUNT(DISTINCT entity_id) FILTER (WHERE schema = 'Person') AS person_entities,
        COUNT(DISTINCT source_country) AS source_country_diversity,
        COUNT(DISTINCT dataset_name) AS authority_diversity
    FROM sanction_edges
    GROUP BY 1, 2
),
with_growth AS (
    SELECT
        month_bucket,
        country,
        total_entities,
        org_entities,
        person_entities,
        GREATEST(
            total_entities - LAG(total_entities, 1, total_entities) OVER (PARTITION BY country ORDER BY month_bucket),
            0
        ) AS recent_growth,
        source_country_diversity,
        authority_diversity
    FROM monthly
)
SELECT
    month_bucket,
    country,
    total_entities,
    org_entities,
    person_entities,
    recent_growth,
    source_country_diversity,
    authority_diversity,
    ROUND(
        total_entities::numeric +
        0.5 * source_country_diversity::numeric +
        0.25 * authority_diversity::numeric +
        0.5 * recent_growth::numeric,
        3
    ) AS spi
FROM with_growth;

/* indexes */
CREATE INDEX IF NOT EXISTS countries_alpha2_idx ON countries(alpha_2);
CREATE INDEX IF NOT EXISTS datasets_name_idx ON datasets(name);
CREATE INDEX IF NOT EXISTS entities_id_idx ON entities(id);
CREATE INDEX IF NOT EXISTS entities_caption_idx ON entities(caption);
CREATE INDEX IF NOT EXISTS entities_caption_lower_idx ON entities(lower(caption));
CREATE INDEX IF NOT EXISTS entities_schema_idx ON entities(schema);
CREATE INDEX IF NOT EXISTS entities_target_idx ON entities(target);
CREATE INDEX IF NOT EXISTS entities_industry_idx ON entities(industry);
CREATE INDEX IF NOT EXISTS entities_first_seen_idx ON entities(first_seen);

CREATE INDEX IF NOT EXISTS entities_countries_id_idx ON entities_countries(id);
CREATE INDEX IF NOT EXISTS entities_countries_caption_idx ON entities_countries(caption);
CREATE INDEX IF NOT EXISTS entities_countries_caption_lower_idx ON entities_countries(lower(caption));
CREATE INDEX IF NOT EXISTS entities_countries_schema_idx ON entities_countries(schema);
CREATE INDEX IF NOT EXISTS entities_countries_target_idx ON entities_countries(target);
CREATE INDEX IF NOT EXISTS entities_countries_industry_idx ON entities_countries(industry);
CREATE INDEX IF NOT EXISTS entities_countries_first_seen_idx ON entities_countries(first_seen);
CREATE INDEX IF NOT EXISTS entities_countries_source_country_idx ON entities_countries(source_country);
CREATE INDEX IF NOT EXISTS entities_countries_target_country_idx ON entities_countries(target_country);

CREATE INDEX IF NOT EXISTS sanction_edges_entity_id_idx ON sanction_edges(entity_id);
CREATE INDEX IF NOT EXISTS sanction_edges_schema_idx ON sanction_edges(schema);
CREATE INDEX IF NOT EXISTS sanction_edges_industry_idx ON sanction_edges(industry);
CREATE INDEX IF NOT EXISTS sanction_edges_first_seen_idx ON sanction_edges(first_seen);
CREATE INDEX IF NOT EXISTS sanction_edges_source_country_idx ON sanction_edges(source_country);
CREATE INDEX IF NOT EXISTS sanction_edges_target_country_idx ON sanction_edges(target_country);
CREATE INDEX IF NOT EXISTS sanction_edges_dataset_name_idx ON sanction_edges(dataset_name);
