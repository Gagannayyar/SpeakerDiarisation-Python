-- dbt model configuration
{{ config(
    materialized='table'
) }}

-- Global Top 30 Manufacturers
WITH _top_manufacturers AS (
    SELECT
        manufacturer,
        'Global Top 30' AS label,
        ROW_NUMBER() OVER (ORDER BY SUM(sku_value_sales_usd) DESC) AS rank,
        GETDATE() AS created_timestamp
    FROM
        DEV_CBI_EMEA.CORE_RAW.T_L0_PYTHIA_SKU_PREVIOUS_DELIVERY
    WHERE
        channel_type = 'Total'
        AND gsr_flag = 1
        AND date_type = 'Monthly'
        AND period IN ('M31', 'M32', 'M33', 'M34', 'M35', 'M36')
        AND sku_value_sales_usd IS NOT NULL
        AND manufacturer IN (
            'Alcon',
            'Bayer/Merck',
            'Beiersdorf',
            'Bioderma Laboratories',
            'Colgate-Palmolive',
            'Coty',
            'Edgewell',
            'Estee Lauder Inc',
            'Haleon/Novartis',
            'Henkel',
            'Jala Group',
            'Kao',
            'Kimberly-Clark',
            'L''Oréal',
            'MacAndrews Forbes Holdings',
            'Mylan',
            'Nestle/Galderma',
            'Nuxe Labs',
            'Perrigo/Omega',
            'Pierre Fabre',
            'Private Label',
            'Procter & Gamble',
            'Proya Cosmetics Co.',
            'Reckitt Benckiser',
            'Sanofi',
            'Shiseido Co.',
            'SPDC',
            'Teva',
            'Unilever',
            'Weleda Company'
        )
    GROUP BY
        manufacturer
)
SELECT
    *
FROM
    _top_manufacturers
ORDER BY
    manufacturer
