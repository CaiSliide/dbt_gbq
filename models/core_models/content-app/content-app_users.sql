{{
    config(
        materialized="incremental",
        unique_key="user_pseudo_id",
        partition_by={
            "field": "acquisition_date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["app_family_name", "app_info_id", "device_model"],
    )
}}


with
    raw_events as (
        select *
        from {{ source("content-app_prod", "events_*") }}
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            -- will append/replace values for yesterday, and the previous 2 days only
            where
                _table_suffix
                between format_date('%Y%m%d', current_date() - 3) and format_date(
                    '%Y%m%d', current_date()
                )
        {% else %}
            where
                _table_suffix
                between '20231210' and format_date('%Y%m%d', current_date())
        {% endif %}

        union all

        select *
        from {{ source("content-app_tmobile_prod", "events_*") }}
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            -- will append/replace values for yesterday, and the previous 2 days only
            where
                _table_suffix
                between format_date('%Y%m%d', current_date() - 3) and format_date(
                    '%Y%m%d', current_date()
                )
        {% else %}
            where
                _table_suffix
                between '20231210' and format_date('%Y%m%d', current_date())
        {% endif %}
    ),
    active_events as (
        select product, event_name, is_active from {{ ref("event_config") }}
    ),

    {% if is_incremental() %}
        historic_user_core as (
            select
                user_pseudo_id,
                acquisition_date,
                app_version_first_seen_string,
                app_version_first_seen_int,
                activation_date
            from {{ this }}
        ),
    {% endif %}

    selecting_raw_events as (
        select
            user_pseudo_id,
            user_id device_id,
            cast(null as string) sliide_backend_profile_id,
            device.advertising_id google_advertising_id,
            app_info.id app_info_id,
            null app_family_name,
            lower(trim(app_info.install_source)) install_source,
            app_info.version app_version_string,
            cast(
                replace(
                    case
                        when
                            regexp_contains(app_info.version, r'[0-9]+\.[0-9]+\.[0-9]+')
                        then
                            concat(
                                lpad(split(app_info.version, '.')[offset (0)], 3, '0'),
                                '.',
                                lpad(split(app_info.version, '.')[offset (1)], 3, '0'),
                                '.',
                                lpad(
                                    regexp_replace(
                                        split(app_info.version, '.')[offset (2)],
                                        r'[^0-9](.*)',
                                        ''
                                    ),
                                    3,
                                    '0'
                                )
                            )
                    end,
                    '.',
                    ''
                ) as int64
            ) app_version_int,
            lower(trim(device.category)) device_type,
            lower(trim(device.mobile_brand_name)) device_make,
            lower(trim(device.mobile_os_hardware_model)) device_model,
            `dbt_production.propertyValueByKeyCoreModels`(
                'screen_layout', user_properties
            ) device_screen_size,
            lower(trim(device.operating_system)) device_os,
            lower(trim(device.operating_system_version)) device_os_version,
            lower(trim(device.language)) device_language,
            cast(device.time_zone_offset_seconds as int64) tz_offset,
            lower(trim(geo.country)) location_country,
            lower(trim(geo.region)) location_state,
            lower(trim(geo.city)) location_city,
            null has_device_firmware_permission,
            parse_date("%Y%m%d", event_date) event_date,
            event_name,
            event_timestamp,
            farm_fingerprint(
                concat(
                    user_pseudo_id,
                    event_name,
                    event_timestamp,
                    row_number() over (
                        partition by user_pseudo_id, event_name, event_timestamp
                    )
                )
            ) row_uid
        from raw_events
    ),
    cm as (
        select
            app_package_name,
            product,
            cast(
                replace(
                    case
                        when regexp_contains(version_from, r'[0-9]+\.[0-9]+\.[0-9]+')
                        then
                            concat(
                                lpad(split(version_from, '.')[offset (0)], 3, '0'),
                                '.',
                                lpad(split(version_from, '.')[offset (1)], 3, '0'),
                                '.',
                                lpad(
                                    regexp_replace(
                                        split(version_from, '.')[offset (2)],
                                        r'[^0-9](.*)',
                                        ''
                                    ),
                                    3,
                                    '0'
                                )
                            )
                    end,
                    '.',
                    ''
                ) as int64
            ) version_from,
            cast(
                replace(
                    case
                        when regexp_contains(version_to, r'[0-9]+\.[0-9]+\.[0-9]+')
                        then
                            concat(
                                lpad(split(version_to, '.')[offset (0)], 3, '0'),
                                '.',
                                lpad(split(version_to, '.')[offset (1)], 3, '0'),
                                '.',
                                lpad(
                                    regexp_replace(
                                        split(version_to, '.')[offset (2)],
                                        r'[^0-9](.*)',
                                        ''
                                    ),
                                    3,
                                    '0'
                                )
                            )
                    end,
                    '.',
                    ''
                ) as int64
            ) version_to
        from {{ ref("client_mapping_events") }}
    ),
    last_row as (
        select distinct
            user_pseudo_id,
            first_value(row_uid) over (
                partition by user_pseudo_id order by event_timestamp desc
            ) row_uid
        from selecting_raw_events
    ),
    int_events as (
        select
            user_pseudo_id,
            device_id,
            sliide_backend_profile_id,
            google_advertising_id,
            app_info_id,
            cm.product app_family_name,
            install_source,
            app_version_string,
            app_version_int,
            device_type,
            device_make,
            device_model,
            cast(
                split(device_screen_size, 'x')[offset(0)] as int64
            ) device_screen_size_x,
            cast(
                split(device_screen_size, 'x')[offset(1)] as int64
            ) device_screen_size_y,
            device_os,
            device_os_version,
            device_language,
            tz_offset / 3600 tz_offset_hours,
            location_country,
            location_state,
            location_city,
            event_date,
            event_timestamp,
            raw.event_name,
            cast(
                case when ae.event_name is not null then true else false end as bool
            ) is_active,
            row_uid
        from selecting_raw_events raw
        left join
            cm
            on raw.app_info_id = cm.app_package_name
            and raw.app_version_int between cm.version_from and cm.version_to
        left join
            active_events ae
            on raw.event_name = ae.event_name
            and cm.product = ae.product
    ),
    activity as (
        select
            user_pseudo_id,
            min(event_date) acquisition_date,
            max(event_date) last_seen_date,
            min(case when is_active then event_date end) activation_date,
            max(case when is_active then event_date end) last_active_date
        from int_events
        group by user_pseudo_id
    ),
    latest_data as (
        select
            user_pseudo_id,
            device_id,
            sliide_backend_profile_id,
            google_advertising_id,
            app_info_id,
            app_family_name,
            install_source,
            app_version_string as app_version_first_seen_string,
            app_version_int as app_version_first_seen_int,
            device_type,
            device_make,
            device_model,
            device_screen_size_x,
            device_screen_size_y,
            device_os,
            device_os_version,
            device_language,
            tz_offset_hours,
            location_country,
            location_state,
            location_city,
            event_name as last_seen_event,
            timestamp_micros(event_timestamp) as last_seen_event_timestamp,
        from int_events
        inner join last_row using (user_pseudo_id, row_uid)
    )
{% if is_incremental() %}
        ,
        historic_compare as (
            select
                user_pseudo_id,
                activity.last_seen_date,
                activity.last_active_date,
                case
                    when his.acquisition_date < activity.acquisition_date
                    then his.acquisition_date
                    else activity.acquisition_date
                end as acquisition_date,
                case
                    when his.activation_date < activity.activation_date
                    then his.activation_date
                    else activity.activation_date
                end as activation_date,
                case
                    when his.acquisition_date < activity.acquisition_date
                    then his.app_version_first_seen_string
                    else latest_data.app_version_first_seen_string
                end as app_version_first_seen_string,
                case
                    when his.acquisition_date < activity.acquisition_date
                    then his.app_version_first_seen_int
                    else latest_data.app_version_first_seen_int
                end as app_version_first_seen_int
            from activity
            left join historic_user_core his using (user_pseudo_id)
            inner join latest_data using (user_pseudo_id)
        )

    select
        h.*,
        d.* except (
            user_pseudo_id, app_version_first_seen_string, app_version_first_seen_int
        )
    from historic_compare h
{% else %} select a.*, d.* except (user_pseudo_id) from activity a
{% endif %}

inner join latest_data d using (user_pseudo_id)
