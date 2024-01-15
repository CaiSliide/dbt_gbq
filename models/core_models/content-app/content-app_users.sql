{{
    config(
        materialized="incremental",
        unique_key="user_pseudo_id",
        partition_by={
            "field": "acquisition_date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["last_seen_app_info_id"],
    )
}}


with
    raw_events as (

        {% set contentapp_sources = [
            "content-app_prod",
            "content-app_tmobile_prod",
            "content-app_mid-tier_prod",
        ] %}
        {% for contentapp_source in contentapp_sources %}
            select *
            from {{ source(contentapp_source, "events_*") }}
            {% if is_incremental() %}
                -- this filter will only be applied on an incremental run
                where
                    _table_suffix
                    between '{{ var("start_date") }}' and '{{ var("end_date") }}'
            {% else %} where _table_suffix between '20221231' and '20221231'
            {% endif %}
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}

    ),
    active_events as (
        select product, event_name, is_active from {{ ref("event_config") }}
    ),

    {% if is_incremental() %}
        historic_user_core as (select * from {{ this }}),
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
            version_from,
            version_to
        from {{ ref('client_mapping_events_unified') }}
    ),
    last_row as (
        select distinct
            user_pseudo_id,
            first_value(row_uid) over (
                partition by user_pseudo_id order by event_timestamp desc
            ) row_uid
        from selecting_raw_events
    ),
    first_row as (
        select distinct
            user_pseudo_id,
            first_value(row_uid) over (
                partition by user_pseudo_id order by event_timestamp
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
            device_id as last_seen_device_id,
            sliide_backend_profile_id as last_seen_sliide_backend_profile_id,
            google_advertising_id as last_seen_google_advertising_id,
            app_info_id as last_seen_app_info_id,
            app_family_name as last_seen_app_family_name,
            app_version_string as last_seen_app_version_string,
            app_version_int as last_seen_app_version_int,
            install_source as last_seen_install_source,
            device_type as last_seen_device_type,
            device_make as last_seen_device_make,
            device_model as last_seen_device_model,
            device_screen_size_x as last_seen_device_screen_size_x,
            device_screen_size_y as last_seen_device_screen_size_y,
            device_os as last_seen_device_os,
            device_os_version as last_seen_device_os_version,
            device_language as last_seen_device_language,
            tz_offset_hours as last_seen_tz_offset_hours,
            location_country as last_seen_location_country,
            location_state as last_seen_location_state,
            location_city as last_seen_location_city,
            event_name as last_seen_event,
            timestamp_micros(event_timestamp) as last_seen_event_timestamp
        from int_events
        inner join last_row using (user_pseudo_id, row_uid)
    ),
    oldest_data as (
        select
            user_pseudo_id,
            app_version_string as first_seen_app_version_string,
            app_version_int as first_seen_app_version_int
        from int_events
        inner join first_row using (user_pseudo_id, row_uid)
    ),
    {% if is_incremental() %}
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
                    then his.first_seen_app_version_string
                    else oldest_data.first_seen_app_version_string
                end as first_seen_app_version_string,
                case
                    when his.acquisition_date < activity.acquisition_date
                    then his.first_seen_app_version_int
                    else oldest_data.first_seen_app_version_int
                end as first_seen_app_version_int,

                {% set fields = [
                    "last_seen_device_id",
                    "last_seen_sliide_backend_profile_id",
                    "last_seen_google_advertising_id",
                    "last_seen_app_info_id",
                    "last_seen_app_family_name",
                    "last_seen_app_version_string",
                    "last_seen_app_version_int",
                    "last_seen_install_source",
                    "last_seen_device_type",
                    "last_seen_device_make",
                    "last_seen_device_model",
                    "last_seen_device_screen_size_x",
                    "last_seen_device_screen_size_y",
                    "last_seen_device_os",
                    "last_seen_device_os_version",
                    "last_seen_device_language",
                    "last_seen_tz_offset_hours",
                    "last_seen_location_country",
                    "last_seen_location_state",
                    "last_seen_location_city",
                    "last_seen_event",
                    "last_seen_event_timestamp",
                ] %}
                {% for field in fields %}
                    case
                        when his.last_seen_date > activity.last_seen_date
                        then his.{{ field }}
                        else latest_data.{{ field }}
                    end as {{ field }}
                    {% if not loop.last %}, {% endif %}
                {% endfor %}
            from activity
            left join historic_user_core his using (user_pseudo_id)
            inner join oldest_data using (user_pseudo_id)
            inner join latest_data using (user_pseudo_id)
        ),
    {% endif %}

    final as (
        {% if is_incremental() %}select h.* from historic_compare h
        {% else %}
            select a.*, l.* except (user_pseudo_id), o.* except (user_pseudo_id)
            from activity a
            inner join oldest_data o using (user_pseudo_id)
            inner join latest_data l using (user_pseudo_id)
        {% endif %}
    )

select
    user_pseudo_id,
    acquisition_date,
    activation_date,
    last_seen_date,
    last_active_date,
    last_seen_event,
    last_seen_event_timestamp,
    last_seen_app_family_name,
    last_seen_app_info_id,
    first_seen_app_version_string,
    first_seen_app_version_int,
    last_seen_app_version_string,
    last_seen_app_version_int,
    last_seen_device_id,
    last_seen_sliide_backend_profile_id,
    last_seen_google_advertising_id,
    last_seen_install_source,
    last_seen_device_type,
    last_seen_device_make,
    last_seen_device_model,
    last_seen_device_screen_size_x,
    last_seen_device_screen_size_y,
    last_seen_device_os,
    last_seen_device_os_version,
    last_seen_device_language,
    last_seen_tz_offset_hours,
    last_seen_location_country,
    last_seen_location_state,
    last_seen_location_city
from final
