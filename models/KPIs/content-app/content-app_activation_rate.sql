with
    yesterday_cohort as (
        select
            acquisition_date,
            user_pseudo_id,
            app_family_name as product,
            app_info_id as flavour,
            app_version_first_seen_string as version,
            device_model,
            device_os_version as operating_system,
            case when activation_date is null or activation_date > acquisition_date + 1 then false else true end as activated
        from {{ ref("content-app_users") }}
        where
            acquisition_date < current_date() - 1
    ),
    minus7_cohort as (
        select
            acquisition_date,
            user_pseudo_id,
            app_family_name as product,
            app_info_id as flavour,
            app_version_first_seen_string as version,
            device_model,
            device_os_version as operating_system,
            case when activation_date is null or activation_date > acquisition_date + 7 then false else true end as activated
        from {{ ref("content-app_users") }}
        where
            acquisition_date < current_date() - 7
    ),
    minus28_cohort as (
        select
            acquisition_date,
            user_pseudo_id,
            app_family_name as product,
            app_info_id as flavour,
            app_version_first_seen_string as version,
            device_model,
            device_os_version as operating_system,
            case when activation_date is null or activation_date > acquisition_date + 28 then false else true end as activated
        from {{ ref("content-app_users") }}
        where
            acquisition_date < current_date() - 28
    )

-- By Flavour breakdown
select
    acquisition_date as date,
    product,
    flavour,
    'total' as version,
    'total' as device_model,
    'total' as operating_system,
    '-1 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from yesterday_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

union all

select
    acquisition_date as date,
    product,
    flavour,
    'total' as version,
    'total' as device_model,
    'total' as operating_system,
    '-7 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus7_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

union all

select
    acquisition_date as date,
    product,
    flavour,
    'total' as version,
    'total' as device_model,
    'total' as operating_system,
    '-28 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus28_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

-- By Flavour & Version breakdown
union all
select
    acquisition_date as date,
    product,
    flavour,
    version,
    'total' as device_model,
    'total' as operating_system,
    '-1 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from yesterday_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

union all

select
    acquisition_date as date,
    product,
    flavour,
    version,
    'total' as device_model,
    'total' as operating_system,
    '-7 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus7_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

union all

select
    acquisition_date as date,
    product,
    flavour,
    version,
    'total' as device_model,
    'total' as operating_system,
    '-28 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus28_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

-- By Flavour & Version & Device breakdown
union all
select
    acquisition_date as date,
    product,
    flavour,
    version,
    device_model,
    'total' as operating_system,
    '-1 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from yesterday_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

union all

select
    acquisition_date as date,
    product,
    flavour,
    version,
    device_model,
    'total' as operating_system,
    '-7 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus7_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

union all

select
    acquisition_date as date,
    product,
    flavour,
    version,
    device_model,
    'total' as operating_system,
    '-28 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus28_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

-- By Flavour & Version & OS breakdown
union all
select
    acquisition_date as date,
    product,
    flavour,
    version,
    'total' as device_model,
    operating_system,
    '-1 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from yesterday_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

union all

select
    acquisition_date as date,
    product,
    flavour,
    version,
    'total' as device_model,
    operating_system,
    '-7 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus7_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system

union all

select
    acquisition_date as date,
    product,
    flavour,
    version,
    'total' as device_model,
    operating_system,
    '-28 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus28_cohort
group by
    acquisition_date,
    cohort,
    product,
    flavour,
    version,
    device_model,
    operating_system