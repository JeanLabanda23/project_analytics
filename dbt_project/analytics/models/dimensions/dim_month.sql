{{ config(materialized='table') }}

with periods as (
    SELECT DISTINCT PERIOD::date as month_start
    from {{ ref('stg_sales_monthly') }}
)

SELECT
    month_start                     as month_date,
    extract(year from month_start)  as year,
    extract(month from month_start) as month,
    to_char(month_start, 'Month')   as month_name
from periods
order by month_start
