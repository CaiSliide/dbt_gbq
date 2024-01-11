with
    base as (
        select distinct
            user_pseudo_id,
            session_start_date,
            app_family_name as app_family_name,
            app_package_name,
            app_version_string,
            device_model,
            device_os_version
        from {{ ref("content-app_sessions") }}
        where
            is_active_session is true
    )

-- By Flavour breakdown
select
    DATE_SUB(base.session_start_date, INTERVAL 27 DAY) as MAU_date_range_start,
    base.session_start_date as MAU_date_range_end,
    base.app_family_name as product,
    base.app_package_name as flavour,
    'total' as version,
    'total' as device_model,
    'total' as operating_system,
    count(base.user_pseudo_id) as mau
from base
join
    base b2
    on b2.session_start_date between date_sub(
        base.session_start_date, interval 27 day
    ) and base.session_start_date
    and base.app_family_name = b2.app_family_name
    and base.app_package_name = b2.app_package_name
    and base.user_pseudo_id = b2.user_pseudo_id
where
    MOD(DATE_DIFF(base.session_start_date, DATE '2023-01-01', DAY), 28) = 0
group by base.session_start_date, base.app_family_name, base.app_package_name

 -- By Flavour & Version breakdown
union all
select
    DATE_SUB(base.session_start_date, INTERVAL 27 DAY) as MAU_date_range_start,
    base.session_start_date as MAU_date_range_end,
    base.app_family_name as product,
    base.app_package_name as flavour,
    base.app_version_string as version,
    'total' as device_model,
    'total' as operating_system,
    count(base.user_pseudo_id) as mau
from base
join
    base b2
    on b2.session_start_date between date_sub(
        base.session_start_date, interval 27 day
    ) and base.session_start_date
    and base.app_family_name = b2.app_family_name
    and base.app_package_name = b2.app_package_name
    and base.user_pseudo_id = b2.user_pseudo_id
    and base.app_version_string = b2.app_version_string
where
    MOD(DATE_DIFF(base.session_start_date, DATE '2023-01-01', DAY), 28) = 0
group by base.session_start_date, base.app_family_name, base.app_package_name, base.app_version_string

-- By Flavour & Version & Device breakdown
    union all
select
    DATE_SUB(base.session_start_date, INTERVAL 27 DAY) as MAU_date_range_start,
    base.session_start_date as MAU_date_range_end,
    base.app_family_name as product,
    base.app_package_name as flavour,
    base.app_version_string as version,
    base.device_model,
    'total' as operating_system,
    count(base.user_pseudo_id) as mau
from base
join
    base b2
    on b2.session_start_date between date_sub(
        base.session_start_date, interval 27 day
    ) and base.session_start_date
    and base.app_family_name = b2.app_family_name
    and base.app_package_name = b2.app_package_name
    and base.user_pseudo_id = b2.user_pseudo_id
    and base.app_version_string = b2.app_version_string
    and base.device_model = b2.device_model
where
    MOD(DATE_DIFF(base.session_start_date, DATE '2023-01-01', DAY), 28) = 0
group by base.session_start_date, base.app_family_name, base.app_package_name, base.app_version_string, base.device_model
-- By Flavour & Version & OS breakdown
union all
select
    DATE_SUB(base.session_start_date, INTERVAL 27 DAY) as MAU_date_range_start,
    base.session_start_date as MAU_date_range_end,
    base.app_family_name as product,
    base.app_package_name as flavour,
    base.app_version_string as version,
    'total' as device_model,
    base.device_os_version as operating_system,
    count(base.user_pseudo_id) as mau
from base
join
    base b2
    on b2.session_start_date between date_sub(
        base.session_start_date, interval 27 day
    ) and base.session_start_date
    and base.app_family_name = b2.app_family_name
    and base.app_package_name = b2.app_package_name
    and base.user_pseudo_id = b2.user_pseudo_id
    and base.app_version_string = b2.app_version_string
    and base.device_os_version = b2.device_os_version
where
    MOD(DATE_DIFF(base.session_start_date, DATE '2023-01-01', DAY), 28) = 0
group by base.session_start_date, base.app_family_name, base.app_package_name, base.app_version_string, base.device_os_version