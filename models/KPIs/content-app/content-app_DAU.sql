-- By Flavour breakdown
select
    session_start_date as date,
    app_family_name as product,
    app_package_name as flavour,
    'total' as version,
    'total' as device_model,
    'total' as operating_system,
    count(distinct user_pseudo_id) as DAU
from {{ ref("content-app_sessions") }}
where
    is_active_session is true
group by
    session_start_date,
    product,
    app_package_name

-- By Flavour & Version breakdown
union all
select
    session_start_date as date,
    app_family_name as product,
    app_package_name as flavour,
    app_version_string as version,
    'total' as device_model,
    'total' as operating_system,
    count(distinct user_pseudo_id) as DAU
from {{ ref("content-app_sessions") }}
where
    is_active_session is true
group by
    session_start_date,
    product,
    app_package_name,
    app_version_string

-- By Flavour & Version & Device breakdown
union all
select
    session_start_date as date,
    app_family_name as product,
    app_package_name as flavour,
    app_version_string as version,
    device_model,
    'total' as operating_system,
    count(distinct user_pseudo_id) as DAU
from {{ ref("content-app_sessions") }}
where
    is_active_session is true
group by
    session_start_date,
    product,
    app_package_name,
    app_version_string,
    device_model

-- By Flavour & Version & OS breakdown
union all
select
    session_start_date as date,
    app_family_name as product,
    app_package_name as flavour,
    app_version_string as version,
    'total' as device_model,
    device_os_version as operating_system,
    count(distinct user_pseudo_id) as DAU
from {{ ref("content-app_sessions") }}
where
    is_active_session is true
group by
    session_start_date,
    product,
    app_package_name,
    app_version_string,
    device_os_version