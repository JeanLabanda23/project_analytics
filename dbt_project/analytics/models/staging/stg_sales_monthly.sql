{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw','sales_monthly') }}

)

select
  corp,
  laboratory,
  lab_mta,
  product,
  tipo_producto,
  prod_ty,
  pack_code,
  product_launch::date    as product_launch,
  pack_launch::date       as pack_launch,
  atc_1,
  atc_3,
  atc_4,
  atc4_cod,
  pack,
  concatenate_molecule_spanish,
  metric,
  period::date            as period,
  metric_value::float     as metric_value
from source