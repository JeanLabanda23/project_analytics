{{ config(materialized='table') }}

with base as (
    select
        product,
        period,
        SUM(metric_value) as total_metric_value
from {{ ref('stg_sales_monthly') }}
group by 1, 2
)

SELECT
    product,
    PERIOD,
    total_metric_value,
    lag(total_metric_value) OVER (PARTITION BY product order by PERIOD) as prev_month_value,
    total_metric_value
     - lag(total_metric_value) over (PARTITION BY product order by PERIOD) as
     month_over_month_change
FROM
base    