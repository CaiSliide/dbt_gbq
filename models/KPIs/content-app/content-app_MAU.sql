with
    base as (
        select distinct
            user_pseudo_id,
            date_trunc(session_start_date, month) as month,
            app_family_name as app_family_name,
            app_package_name,
            app_version_string,
            device_model,
            device_os_version
        from {{ ref("content-app_sessions") }}
        where is_active_session is true and extract(day from session_start_date) <= 28
    )

-- By Flavour breakdown
select
    month,
    app_family_name as product,
    app_package_name as flavour,
    'total' as version,
    'total' as device_model,
    'total' as operating_system,
    count(distinct user_pseudo_id) as mau
from base
group by month, app_family_name, app_package_name

-- By Flavour & Version breakdown
union all
select
    month,
    app_family_name as product,
    app_package_name as flavour,
    app_version_string as version,
    'total' as device_model,
    'total' as operating_system,
    count(distinct user_pseudo_id) as mau
from base
group by month, app_family_name, app_package_name, app_version_string

-- By Flavour & Version & Device breakdown
union all
select
    month,
    app_family_name as product,
    app_package_name as flavour,
    app_version_string as version,
    device_model,
    'total' as operating_system,
    count(distinct user_pseudo_id) as mau
from base
group by month, app_family_name, app_package_name, app_version_string, device_model
-- By Flavour & Version & OS breakdown
union all
select
    month,
    app_family_name as product,
    app_package_name as flavour,
    app_version_string as version,
    'total' as device_model,
    device_os_version as operating_system,
    count(distinct user_pseudo_id) as mau
from base
group by month, app_family_name, app_package_name, app_version_string, device_os_version
