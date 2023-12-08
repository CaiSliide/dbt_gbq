{{
    config(
        materialized='incremental',
        unique_key='event_date') 
}}

with 
active_events as (
    select event_name from {{ ref('stg_event_config') }} where is_active = True
),
source_events as (
    select 
        event_date,
        event_name,
        user_pseudo_id
    from {{ source('content_app_prod', 'events_*')}}
    
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    -- will append/replace values for yesterday, and the previous 2 days only
    where _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', CURRENT_DATE() - 3) AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    {% endif %}
 
)

select
    PARSE_DATE('%Y%m%d', event_date) as event_date,
    count(distinct user_pseudo_id) as DAU
from source_events
where
    event_name in (select * from active_events)

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    -- will append/replace values for yesterday, and the previous 2 days only
    and PARSE_DATE('%Y%m%d', event_date) between (select DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) and (select DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    {% endif %}

group by
    event_date
order by 
    event_date