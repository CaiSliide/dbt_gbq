{{
    config(
        materialized="incremental",
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["app_info_id", "app_info_version", "event_name"],
        pre_hook="delete from {{ this }} where event_date = current_date() - 3",
    ) 
}}

{% set product = "contentapp" %}

with
    raw_events as (
        select *
        from {{ source("content-app_prod", "events_*") }}
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            -- will append/replace values for yesterday, and the previous 2 days only
            -- where _table_suffix = format_date('%Y%m%d', current_date() - 3)
            where _table_suffix between '20231101' and '20231130'
        {% else %}
            where
                _table_suffix
                -- between '20231001' and format_date('%Y%m%d', current_date())
                between '20231001' and '20231031'
        {% endif %}

        union all

        select *
        from {{ source("content-app_tmobile_prod", "events_*") }}
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            -- will append/replace values for yesterday, and the previous 2 days only
            -- where _table_suffix = format_date('%Y%m%d', current_date() - 3)
            where _table_suffix between '20231101' and '20231218'
        {% else %}
            where
                _table_suffix
                -- between '20231001' and format_date('%Y%m%d', current_date())
                between '20231001' and '20231031'
        {% endif %}
    ),
    session_events as (
        select product, event_name
        from {{ ref("event_config") }}
        where product = '{{ product }}' and is_session
    ),
    session_params as (
        select 1000000 * session_time session_time  -- conversion to microseconds
        from {{ ref("session_parameters") }}
        where product = '{{ product }}'
    ),
    selecting_raw_events as (
        select
            /*
            if time between event and previous is greater than the max time interval then it is the first event in the session
            a coalesce has been added here to make sure that a user's very first event for the time period does not
            affect the calculation and set the period's second event as the session's first event
            */
            coalesce(
                event_timestamp - lag(event_timestamp) over (
                    partition by app_info.id, user_pseudo_id
                    order by event_timestamp, event_bundle_sequence_id
                ),
                99999999999
            )
            > (select session_time from session_params) first_event_in_session,
            app_info.id app_info_id,
            app_info.version app_info_version,
            app_info.firebase_app_id firebase_app_id,
            app_info.install_source install_source,
            * except (app_info)
        from raw_events
        where event_name in (select distinct event_name from session_events)
    ),
    session_id_calc as (
        select
            case
                when first_event_in_session
                then
                    farm_fingerprint(
                        concat(user_pseudo_id, app_info_id, event_timestamp)
                    )
                else null
            end session_id,
            *
        from selecting_raw_events
    ),
    event_id_calc as (
        select
            farm_fingerprint(
                concat(user_pseudo_id, event_timestamp, event_name)
            ) event_id,
            *
        from session_id_calc
    )
select
    last_value(session_id ignore nulls) over (
        partition by app_info_id, user_pseudo_id
        order by event_timestamp, event_bundle_sequence_id
    ) session_id,
    parse_date('%Y%m%d', event_date) event_date,
    * except (event_date, session_id, first_event_in_session)
from event_id_calc

