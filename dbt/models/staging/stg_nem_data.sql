{{ config(materialized='view') }}

with source as (
    select * from {{ source('aemo_electricity', 'nem_partitioned') }}
),

renamed as (
    select
        settlement_date,
        region_id,
        total_demand_mw,
        price_aud_per_mwh,
        period_type,
        DATE(settlement_date)          as settlement_date_day,
        EXTRACT(YEAR FROM settlement_date)  as year,
        EXTRACT(MONTH FROM settlement_date) as month,
        EXTRACT(HOUR FROM settlement_date)  as hour
    from source
    where settlement_date is not null
      and total_demand_mw > 0
      and price_aud_per_mwh is not null
)

select * from renamed
