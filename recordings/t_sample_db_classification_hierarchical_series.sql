-- models/my_project/hierarchical_series.sql

{{
    config(
        materialized = 'table'
    )
}}

-- Hierarchical series for classification training (all calendars)
WITH _category_sales AS (
    SELECT
        market,
        supplier,
        source,
        channel,
        category,
        calendar,
        frequency,
        last_period,
        true_date,
        unit_sales,
        value_sales_lcu,
        value_sales_usd,
        volume_sales
    FROM
        {{ ref('t_sample_db_aggregated_test.sql') }}
    WHERE
        level_name = 'Category'
)

SELECT
    md5(l.market || l.supplier || l.source || l.channel || l.category || l.manufacturer || l.brand || l.frequency || l.last_period) AS series_id,
    l.*,
    CASE
        WHEN c.unit_sales > 0 THEN l.unit_sales / c.unit_sales
        ELSE NULL
    END AS unit_share,
    CASE
        WHEN c.value_sales_lcu > 0 THEN l.value_sales_lcu / c.value_sales_lcu
        ELSE NULL
    END AS value_share,
    CASE
        WHEN c.volume_sales > 0 THEN l.volume_sales / c.volume_sales
        ELSE NULL
    END AS volume_share,
    CASE
        WHEN l.unit_sales > 0 THEN l.value_sales_lcu / l.unit_sales
        ELSE NULL
    END AS unit_price_lcu,
    CASE
        WHEN l.unit_sales > 0 THEN l.value_sales_usd / l.unit_sales
        ELSE NULL
    END AS unit_price_usd,
    CASE
        WHEN l.volume_sales > 0 THEN l.value_sales_lcu / l.volume_sales
        ELSE NULL
    END AS volume_price_lcu,
    CASE
        WHEN l.volume_sales > 0 THEN l.value_sales_usd / l.volume_sales
        ELSE NULL
    END AS volume_price_usd,
    CASE
        WHEN l.unit_sales > 0 AND c.value_sales_usd > 0 AND c.unit_sales > 0 THEN
            ((l.value_sales_usd / l.unit_sales) - (c.value_sales_usd / c.unit_sales)) / (c.value_sales_usd / c.unit_sales)
        ELSE NULL
    END AS relative_unit_price,
    CASE
        WHEN l.volume_sales > 0 AND c.value_sales_usd > 0 AND c.volume_sales > 0 THEN
            ((l.value_sales_usd / l.volume_sales) - (c.value_sales_usd / c.volume_sales)) / (c.value_sales_usd / c.volume_sales)
        ELSE NULL
    END AS relative_volume_price,
    getdate() AS created_timestamp
FROM
    {{ ref('t_sample_db_aggregated_test.sql') }} AS l
JOIN
    _category_sales AS c
ON
    l.market = c.market
    AND
    l.supplier = c.supplier
    AND
    l.source = c.source
    AND
    l.channel = c.channel
    AND
    l.category = c.category
    AND
    l.calendar = c.calendar
    AND
    l.frequency = c.frequency
    AND
    l.last_period = c.last_period
    AND
    l.true_date = c.true_date
ORDER BY
    l.region,
    l.market,
    l.source,
    l.channel,
    l.category,
    l.calendar,
    l.last_period,
    l.level_number,
    l.manufacturer,
    l.brand,
    l.period;
