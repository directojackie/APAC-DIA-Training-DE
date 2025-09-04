{{
  config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='merge'
  )
}}

with src as (
  select * from {{ source('bronze', 'orders_lines') }}
),
typed as (
  select
    cast(order_id as bigint) as order_id,
    cast(line_number as int) as line_number,
    cast(product_id as bigint) as product_id,
    cast(qty as int) as quantity,
    cast(unit_price as numeric(12,4)) as unit_price,
    cast(line_discount_pct as numeric(5,4)) as line_discount_pct,
    cast(tax_pct as numeric(5,4)) as tax_pct,
    ingestion_ts
  from src
)

select * from typed

{% if is_incremental() %}
  where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
