-- dbt model configuration
{{ config(
    materialized='table'
) }}

-- aggregate sales for Kenvue brands and top 10 competitors (all calendars)
WITH _brand_sales AS (
    SELECT
        region,
        cluster,
        country_alpha_2,
        country_alpha_3,
        market,
        supplier,
        source,
        channel,
        gfo,
        need_state,
        category,
        CASE
            WHEN manufacturer = 'Kenvue' THEN manufacturer
            WHEN manufacturer IN (
                SELECT
                    manufacturer
                FROM
                    DEV_CBI_GLOBAL.CORE_RAW.t_pythia_l1_reference_global_competitors
                WHERE
                    rank <= 10
            ) THEN manufacturer
            ELSE 'All Other Manufacturers'
        END AS manufacturer,
        CASE
            WHEN manufacturer = 'Kenvue' THEN brand
            WHEN manufacturer IN (
                SELECT
                    manufacturer
                FROM
                    DEV_CBI_GLOBAL.CORE_RAW.t_pythia_l1_reference_global_competitors
                WHERE
                    rank <= 10
            ) THEN manufacturer || ' Brands'
            ELSE 'All Other Brands'
        END AS brand,
        calendar,
        frequency,
        year,
        period,
        last_period,
        true_date,
        report_date,
        unit_sales,
        value_sales_lcu,
        value_sales_usd,
        volume_sales,
        delivery_timestamp
    FROM
        DEV_CBI_GLOBAL.CORE_RAW.T_SAMPLE_DB_BRAND_SALES_TEST
)

SELECT
    region,
    cluster,
    country_alpha_2,
    country_alpha_3,
    market,
    supplier,
    source,
    channel,
    gfo,
    need_state,
    category,
    manufacturer,
    brand,
    calendar,
    frequency,
    year,
    period,
    last_period,
    true_date,
    report_date,
    SUM(unit_sales) AS unit_sales,
    SUM(value_sales_lcu) AS value_sales_lcu,
    SUM(value_sales_usd) AS value_sales_usd,
    SUM(volume_sales) AS volume_sales,
    MAX(delivery_timestamp) AS delivery_timestamp,
    GETDATE() AS created_timestamp
FROM
    _brand_sales
GROUP BY
    region,
    cluster,
    country_alpha_2,
    country_alpha_3,
    market,
    supplier,
    source,
    channel,
    gfo,
    need_state,
    category,
    manufacturer,
    brand,
    calendar,
    frequency,
    year,
    period,
    last_period,
    true_date,
    report_date
ORDER BY
    region,
    market,
    source,
    channel,
    category,
    calendar,
    last_period,
    manufacturer,
    brand,
    period
