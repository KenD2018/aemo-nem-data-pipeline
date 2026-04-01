{{ config(
    materialized='table',
    partition_by={
        "field": "settlement_date_day",
        "data_type": "date"
    },
    cluster_by=["region_id"]
) }}

with daily_agg as (
    select
        settlement_date_day,
        region_id,
        year,
        month,
        avg(price_aud_per_mwh)  as avg_price_aud_per_mwh,
        max(price_aud_per_mwh)  as max_price_aud_per_mwh,
        min(price_aud_per_mwh)  as min_price_aud_per_mwh,
        avg(total_demand_mw)    as avg_demand_mw,
        max(total_demand_mw)    as max_demand_mw,
        count(*)                as interval_count
    from {{ ref('stg_nem_data') }}
    group by 1, 2, 3, 4
)

select * from daily_agg
