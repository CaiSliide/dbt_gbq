with
    yesterday_cohort as (
        select
            acquisition_date,
            user_pseudo_id,
            case when activation_date is null or activation_date > acquisition_date + 1 then false else true end as activated
        from {{ ref("content-app_users") }}
        where
            acquisition_date < current_date() - 1
    ),
    minus7_cohort as (
        select
            acquisition_date,
            user_pseudo_id,
            case when activation_date is null or activation_date > acquisition_date + 7 then false else true end as activated
        from {{ ref("content-app_users") }}
        where
            acquisition_date < current_date() - 7
    ),
    minus28_cohort as (
        select
            acquisition_date,
            user_pseudo_id,
            case when activation_date is null or activation_date > acquisition_date + 28 then false else true end as activated
        from {{ ref("content-app_users") }}
        where
            acquisition_date < current_date() - 28
    )


select
    acquisition_date as date,
    '-1 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from yesterday_cohort
group by
    acquisition_date,
    cohort

union all

select
    acquisition_date as date,
    '-7 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus7_cohort
group by
    acquisition_date,
    cohort

union all

select
    acquisition_date as date,
    '-28 day' as cohort,
    count(distinct user_pseudo_id) as acqusitions,
    count(distinct case when activated is true then user_pseudo_id end) as activations,
    count(distinct case when activated is true then user_pseudo_id end)
    / count(distinct user_pseudo_id) as activation_rate
from minus28_cohort
group by
    acquisition_date,
    cohort