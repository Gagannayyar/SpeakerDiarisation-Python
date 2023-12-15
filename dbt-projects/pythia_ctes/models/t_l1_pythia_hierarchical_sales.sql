-- models/my_project/hierarchical_sales.sql

{{
    config(
        materialized = 'table'
    )
}}

-- Hierarchical sales for Kenvue brands and top 10 competitors (all calendars)
WITH _multibrand_and_competitor_manufacturers AS (
    SELECT
        market,
        supplier,
        source,
        channel,
        category,
        manufacturer,
        calendar,
        frequency,
        last_period
    FROM
        {{ ref('t_l1_pythia_aggregated_sales') }}
    GROUP BY
        market,
        supplier,
        source,
        channel,
        category,
        manufacturer,
        calendar,
        frequency,
        last_period
    HAVING
        COUNT(DISTINCT brand) > 1
    UNION ALL
    SELECT
        market,
        supplier,
        source,
        channel,
        category,
        manufacturer,
        calendar,
        frequency,
        last_period
    FROM
        {{ ref('t_l1_pythia_aggregated_sales') }}
    WHERE
        manufacturer <> 'Kenvue'
    GROUP BY
        market,
        supplier,
        source,
        channel,
        category,
        manufacturer,
        calendar,
        frequency,
        last_period
    HAVING
        COUNT(DISTINCT brand) = 1
),
_category_level AS (
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
        category || ' Manufacturers' AS manufacturer,
        category || ' Brands' AS brand,
        'Category' AS level_name,
        CAST(1 AS SMALLINT) AS level_number,
        TRUE AS root_node,
        FALSE AS leaf_node,
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
        MAX(delivery_timestamp) AS delivery_timestamp
    FROM
        {{ ref('t_l1_pythia_aggregated_sales') }}
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
        calendar,
        frequency,
        year,
        period,
        last_period,
        true_date,
        report_date
),
_manufacturer_level AS (
    SELECT
        x.region,
        x.cluster,
        x.country_alpha_2,
        x.country_alpha_3,
        x.market,
        x.supplier,
        x.source,
        x.channel,
        x.gfo,
        x.need_state,
        x.category,
        x.manufacturer,
        x.manufacturer || ' Brands' AS brand,
        'Manufacturer' AS level_name,
        CAST(2 AS SMALLINT) AS level_number,
        FALSE AS root_node,
        CASE
            WHEN x.manufacturer = 'Kenvue' THEN FALSE
            ELSE TRUE
        END AS leaf_node,
        x.calendar,
        x.frequency,
        x.year,
        x.period,
        x.last_period,
        x.true_date,
        x.report_date,
        SUM(x.unit_sales) AS unit_sales,
        SUM(x.value_sales_lcu) AS value_sales_lcu,
        SUM(x.value_sales_usd) AS value_sales_usd,
        SUM(x.volume_sales) AS volume_sales,
        MAX(x.delivery_timestamp) AS delivery_timestamp
    FROM
        {{ ref('t_l1_pythia_aggregated_sales') }} AS x
        JOIN
        _multibrand_and_competitor_manufacturers AS y
        ON
        x.market = y.market
        AND
        x.supplier = y.supplier
        AND
        x.source = y.source
        AND
        x.channel = y.channel
        AND
        x.category = y.category
        AND
        x.manufacturer = y.manufacturer
        AND
        x.calendar = y.calendar
        AND
        x.frequency = y.frequency
        AND
        x.last_period = y.last_period
    GROUP BY
        x.region,
        x.cluster,
        x.country_alpha_2,
        x.country_alpha_3,
        x.market,
        x.supplier,
        x.source,
        x.channel,
        x.gfo,
        x.need_state,
        x.category,
        x.manufacturer,
        x.calendar,
        x.frequency,
        x.year,
        x.period,
        x.last_period,
        x.true_date,
        x.report_date
),
_brand_level AS (
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
        CASE
            WHEN brand = 'All Other Derived Kenvue' THEN 'Derived Kenvue Brands'
            WHEN brand = 'All Other Kenvue - Unknown' THEN 'Unknown Kenvue Brands'
            ELSE brand
        END AS brand,
        'Brand' AS level_name,
        CAST(3 AS SMALLINT) AS level_number,
        FALSE AS root_node,
        TRUE AS leaf_node,
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
        {{ ref('t_l1_pythia_aggregated_sales') }}
    WHERE
        manufacturer = 'Kenvue'
),
_levels AS (
    SELECT * FROM _category_level
    UNION ALL
    SELECT * FROM _manufacturer_level
    UNION ALL
    SELECT * FROM _brand_level
)
SELECT
    md5(market || supplier || source || channel || category || manufacturer || brand || frequency || last_period) AS series_id,
    *,
    GETDATE() AS created_timestamp
FROM
    _levels
ORDER BY
    region,
    market,
    source,
    channel,
    category,
    last_period,
    level_number,
    manufacturer,
    brand,
    period
