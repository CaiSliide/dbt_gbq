{{
    config(
        materialized="incremental",
        unique_key="session_id",
        partition_by={
            "field": "session_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["app_package_name", "is_active_session", "app_version_int", "device_model"],
    )
}}

with base as (
    select
        session_id
        , min(user_pseudo_id) user_pseudo_id
        , min(event_date) session_start_date
        , min(event_timestamp) session_start_timestamp
        , max(event_timestamp) session_end_timestamp
        , min(lower(trim(device.category))) device_type
        , min(lower(trim(device.mobile_brand_name))) device_make
        , min(lower(trim(device.mobile_os_hardware_model))) device_model
        , min(lower(trim(device.operating_system))) device_os
        , min(lower(trim(device.operating_system_version))) device_os_version
        , min(lower(trim(geo.country))) location_country
        , min(lower(trim(geo.region))) location_state
        , min(lower(trim(geo.city))) location_city
        , min(lower(trim(app_info_id))) app_package_name
        , min(lower(trim(cm.product))) app_family_name
        , min(lower(trim(app_info_version))) app_version_string
        , min(cast(replace(case
            when
                regexp_contains(app_info_version, r'[0-9]+\.[0-9]+\.[0-9]+')
            then
                concat(
                    lpad(split(app_info_version, '.')[offset (0)], 3, '0'),
                    '.',
                    lpad(split(app_info_version, '.')[offset (1)], 3, '0'),
                    '.',
                    lpad(regexp_replace(split(app_info_version, '.')[offset (2)],  r'[^0-9](.*)', ''), 3, '0')
                )
        end, '.', '') as int64)) app_version_int
        , min(cast(device.time_zone_offset_seconds as int64)/3600) tz_offset_hours
        , max(ec.is_active) is_active_session
    from
        {{ ref("headlines_events") }} c
        left join {{ ref("client_mapping_events_version") }} cm
            on lower(trim(c.app_info_id)) = cm.app_package_name
            and cast(replace(case
                when
                    regexp_contains(c.app_info_version, r'[0-9]+\.[0-9]+\.[0-9]+')
                then
                    concat(
                        lpad(split(c.app_info_version, '.')[offset (0)], 3, '0'),
                        '.',
                        lpad(split(c.app_info_version, '.')[offset (1)], 3, '0'),
                        '.',
                        lpad(regexp_replace(split(c.app_info_version, '.')[offset (2)],  r'[^0-9](.*)', ''), 3, '0')
                    )
            end, '.', '') as int64) between cm.version_from_num_long and cm.version_to_num_long
        left join {{ ref("event_config") }} ec
            on ec.product = cm.product
            and ec.event_name = c.event_name
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            -- will append/replace values for yesterday, and the previous 2 days only
            where
                event_date between (current_date() - 3) and current_date()
        {% else %}
            where
                event_date between '2023-10-01' and '2023-10-31'
        {% endif %}

    group by
        session_id
)
select
    session_id
    , user_pseudo_id
    , session_start_date
    , timestamp_micros(session_start_timestamp) session_start_timestamp
    , timestamp_micros(session_end_timestamp) session_end_timestamp
    , device_type
    , device_make
    , device_model
    , device_os
    , device_os_version
    , location_country
    , location_state
    , location_city
    , base.app_package_name
    , app_family_name
    , app_version_string
    , app_version_int
    , tz_offset_hours
    , is_active_session
from
    base