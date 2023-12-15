-- your_model.sql

-- dbt model configuration
{{ config(
    materialized='table'
) }}

WITH _selection AS (
    SELECT
        *,
        CASE
            WHEN date_type = 'Weekly' THEN 'WEEK'
            WHEN time_period_type = 'P - Period (only for IRI e.g. 4-4-4 data)' THEN 'MONTH_444'
            WHEN time_period_type = 'F - Fiscal (-14)' THEN 'MONTH_445'
            ELSE 'MONTH'
        END AS calendar,
        CASE
            WHEN date_type = 'Weekly' THEN TO_VARCHAR(real_date, 'W-DY')
            WHEN time_period_type = 'P - Period (only for IRI e.g. 4-4-4 data)' THEN TO_VARCHAR(real_date, '4W-DY')
            WHEN time_period_type = 'F - Fiscal (-14)' THEN TO_VARCHAR(real_date, '4W/5W-DY')
            ELSE 'M'
        END AS frequency
    FROM
        DEV_CBI_GLOBAL.CORE_RAW.T_SAMPLE_DBT_TEST
    WHERE
        (
            -- by default include total channel type
            channel_type = 'Total'
            -- include KAD in exceptions where channel splits sum to total correctly
            OR (channel_type = 'KAD' AND market = 'China' AND channel_source = 'E-Commerce')
        )
        AND gsr_flag = 1
        AND NOT (market = 'Italy' AND channel_description = 'e-Pharma' AND date_type = 'Weekly')
        AND NOT (channel_type = 'Total' AND market = 'China' AND channel_source = 'E-Commerce')
),
_product_sales AS (
    SELECT
        ggh_region AS region,
        ggh_cluster AS cluster,
        ggh_country_2_cd AS country_alpha_2,
        ggh_country_3_cd AS country_alpha_3,
        market,
        supplier,
        channel_source AS source,
        channel_description AS channel,
        gfo,
        need_state,
        category,
        manufacturer,
        brand,
        calendar,
        frequency,
        CASE
            WHEN calendar = 'WEEK' THEN TRY_CAST(TO_VARCHAR(real_date, 'IYYY') AS INTEGER)
            WHEN calendar = 'MONTH_444' THEN
                CASE
                    WHEN EXTRACT(WEEK FROM real_date) < 2 THEN EXTRACT(YEAR FROM real_date) - 1
                    ELSE TRY_CAST(TO_VARCHAR(real_date, 'IYYY') AS INTEGER)
                END
            ELSE TRY_CAST(TO_VARCHAR(time_period, 'YYYY') AS INTEGER)
        END AS year,
        CASE
            WHEN calendar = 'WEEK' THEN TO_VARCHAR(real_date, 'IYYY"-W"IW')
            WHEN calendar = 'MONTH_444' THEN
                CASE
                    WHEN EXTRACT(WEEK FROM real_date) < 2 THEN TO_VARCHAR(DATEADD(MONTH, -1, real_date), 'IYYY"-13"')
                    WHEN EXTRACT(WEEK FROM real_date) < 6 THEN TO_VARCHAR(real_date, 'IYYY"-01"')
                    WHEN EXTRACT(WEEK FROM real_date) < 10 THEN TO_VARCHAR(real_date, 'IYYY"-02"')
                    WHEN EXTRACT(WEEK FROM real_date) < 14 THEN TO_VARCHAR(real_date, 'IYYY"-03"')
                    WHEN EXTRACT(WEEK FROM real_date) < 18 THEN TO_VARCHAR(real_date, 'IYYY"-04"')
                    WHEN EXTRACT(WEEK FROM real_date) < 22 THEN TO_VARCHAR(real_date, 'IYYY"-05"')
                    WHEN EXTRACT(WEEK FROM real_date) < 26 THEN TO_VARCHAR(real_date, 'IYYY"-06"')
                    WHEN EXTRACT(WEEK FROM real_date) < 30 THEN TO_VARCHAR(real_date, 'IYYY"-07"')
                    WHEN EXTRACT(WEEK FROM real_date) < 34 THEN TO_VARCHAR(real_date, 'IYYY"-08"')
                    WHEN EXTRACT(WEEK FROM real_date) < 38 THEN TO_VARCHAR(real_date, 'IYYY"-09"')
                    WHEN EXTRACT(WEEK FROM real_date) < 42 THEN TO_VARCHAR(real_date, 'IYYY"-10"')
                    WHEN EXTRACT(WEEK FROM real_date) < 46 THEN TO_VARCHAR(real_date, 'IYYY"-11"')
                    WHEN EXTRACT(WEEK FROM real_date) < 50 THEN TO_VARCHAR(real_date, 'IYYY"-12"')
                    WHEN EXTRACT(WEEK FROM real_date) < 54 THEN TO_VARCHAR(real_date, 'IYYY"-13"')
                END
            ELSE TO_VARCHAR(time_period, 'YYYY"-"MM')
        END AS period,
        CASE
            WHEN calendar = 'WEEK' THEN
                MAX(TO_VARCHAR(time_period, 'IYYY"-W"IW')) OVER (
                    PARTITION BY
                        market,
                        supplier,
                        source,
                        channel,
                        category,
                        file_time_stamp,
                        calendar,
                        frequency
                )
            WHEN calendar = 'MONTH_444' THEN
                CASE
                    WHEN EXTRACT(WEEK FROM last_period) < 2 THEN TO_VARCHAR(DATEADD(MONTH, -1, last_period), 'IYYY"-13"')
                    WHEN EXTRACT(WEEK FROM last_period) < 6 THEN TO_VARCHAR(last_period, 'IYYY"-01"')
                    WHEN EXTRACT(WEEK FROM last_period) < 10 THEN TO_VARCHAR(last_period, 'IYYY"-02"')
                    WHEN EXTRACT(WEEK FROM last_period) < 14 THEN TO_VARCHAR(last_period, 'IYYY"-03"')
                    WHEN EXTRACT(WEEK FROM last_period) < 18 THEN TO_VARCHAR(last_period, 'IYYY"-04"')
                    WHEN EXTRACT(WEEK FROM last_period) < 22 THEN TO_VARCHAR(last_period, 'IYYY"-05"')
                    WHEN EXTRACT(WEEK FROM last_period) < 26 THEN TO_VARCHAR(last_period, 'IYYY"-06"')
                    WHEN EXTRACT(WEEK FROM last_period) < 30 THEN TO_VARCHAR(last_period, 'IYYY"-07"')
                    WHEN EXTRACT(WEEK FROM last_period) < 34 THEN TO_VARCHAR(last_period, 'IYYY"-08"')
                    WHEN EXTRACT(WEEK FROM last_period) < 38 THEN TO_VARCHAR(last_period, 'IYYY"-09"')
                    WHEN EXTRACT(WEEK FROM last_period) < 42 THEN TO_VARCHAR(last_period, 'IYYY"-10"')
                    WHEN EXTRACT(WEEK FROM last_period) < 46 THEN TO_VARCHAR(last_period, 'IYYY"-11"')
                    WHEN EXTRACT(WEEK FROM last_period) < 50 THEN TO_VARCHAR(last_period, 'IYYY"-12"')
                    WHEN EXTRACT(WEEK FROM last_period) < 54 THEN TO_VARCHAR(last_period, 'IYYY"-13"')
                END
            ELSE TO_VARCHAR(last_period, 'YYYY"-"MM')
        END AS last_period,
        real_date AS true_date,
        CASE
            WHEN calendar = 'WEEK' THEN
                CASE
                    WHEN TO_VARCHAR(real_date, 'IYYY') IN ('2020', '2026') THEN
                        CASE
                            WHEN EXTRACT(WEEK FROM real_date) < 2 THEN TO_VARCHAR(LAST_DAY(DATEADD('MONTH', -1, TO_DATE(EXTRACT(YEAR FROM real_date) || '-12-01', 'YYYY-MM-DD'))), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 6 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-01"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 10 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-02"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 15 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-03"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 19 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-04"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 23 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-05"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 28 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-06"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 32 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-07"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 36 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-08"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 41 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-09"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 45 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-10"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 49 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-11"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 54 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-12"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                        END
                    ELSE
                        CASE
                            WHEN EXTRACT(WEEK FROM real_date) < 5 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-01"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 9 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-02"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 14 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-03"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 18 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-04"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 22 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-05"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 27 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-06"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 31 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-07"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 35 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-08"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 40 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-09"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 44 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-10"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 48 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-11"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                            WHEN EXTRACT(WEEK FROM real_date) < 53 THEN TO_VARCHAR(LAST_DAY(TO_DATE(TO_VARCHAR(real_date, 'IYYY"-12"'), 'YYYY-MM-DD')), 'YYYY-MM-DD')
                        END
                END
            ELSE TO_VARCHAR(LAST_DAY(time_period), 'YYYY-MM-DD')
        END AS report_date,
        sku_unit_sales AS unit_sales,
        sku_value_sales_lc AS value_sales_lcu,
        sku_value_sales_usd AS value_sales_usd,
        sku_volume_sales AS volume_sales,
        file_time_stamp AS delivery_timestamp
    FROM
        _selection
),
_brand_sales AS (
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
        sum(unit_sales) AS unit_sales,
        sum(value_sales_lcu) AS value_sales_lcu,
        sum(value_sales_usd) AS value_sales_usd,
        sum(volume_sales) AS volume_sales,
        max(delivery_timestamp) AS delivery_timestamp
    FROM
        _product_sales
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
),
_brand_periods AS (
    SELECT
        y.region,
        y.cluster,
        y.country_alpha_2,
        y.country_alpha_3,
        y.market,
        y.supplier,
        y.source,
        y.channel,
        y.gfo,
        y.need_state,
        y.category,
        y.manufacturer,
        y.brand,
        x.calendar,
        x.frequency,
        x.year,
        x.period,
        x.last_period,
        x.true_date,
        x.report_date,
        y.delivery_timestamp
    FROM
        (
            -- get periods across channel categories with same last period
            SELECT DISTINCT
                market,
                supplier,
                source,
                channel,
                calendar,
                frequency,
                year,
                period,
                last_period,
                true_date,
                report_date
            FROM
                _brand_sales
        ) AS x
        JOIN
        (
            -- brands
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
                last_period,
                max(delivery_timestamp) AS delivery_timestamp  -- a brand in different segments can have same last_period but different timestamps
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
                last_period
        ) AS y
        ON
        x.market = y.market
        AND
        x.supplier = y.supplier
        AND
        x.source = y.source
        AND
        x.channel = y.channel
        AND
        x.calendar = y.calendar
        AND
        x.frequency = y.frequency
        AND
        x.last_period = y.last_period
)
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
    x.brand,
    x.calendar,
    x.frequency,
    x.year,
    x.period,
    x.last_period,
    x.true_date,
    x.report_date,
    y.unit_sales,
    y.value_sales_lcu,
    y.value_sales_usd,
    y.volume_sales,
    x.delivery_timestamp,
    getdate() AS created_timestamp
FROM
    _brand_periods AS x
    LEFT JOIN
    _brand_sales AS y
    ON
    x.market = y.market
    AND
    x.supplier = y.supplier
    AND
    y.source = y.source
    AND
    x.channel = y.channel
    AND
    x.category = y.category
    AND
    x.manufacturer = y.manufacturer
    AND
    x.brand = y.brand
    AND
    x.calendar = y.calendar
    AND
    x.frequency = y.frequency
    AND
    x.last_period = y.last_period
    AND
    x.true_date = y.true_date
ORDER BY
    x.region,
    x.market,
    x.source,
    x.channel,
    x.category,
    x.calendar,
    x.last_period,
    x.manufacturer,
    x.brand,
    x.period