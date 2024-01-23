with
    source as (
        select user_pseudo_id, app_package_name, session_start_date as date
        from {{ ref("headlines_sessions") }}
        where
            session_start_date between current_date() - 50 and current_date()
            and is_active_session is true
            and user_pseudo_id is not null
    ),
    date_range as (select distinct date as date from source),
    users as (select distinct user_pseudo_id, app_package_name from source),
    user_active_dates as (
        select distinct user_pseudo_id, app_package_name, date as date from source
    ),
    user_all_dates as (
        select date, user_pseudo_id, app_package_name from date_range cross join users
    ),
    user_all_dates_is_active as (
        select
            all_d.date,
            all_d.user_pseudo_id,
            all_d.app_package_name,
            case when active.date is not null then true else false end as active_date
        from user_all_dates all_d
        left join
            user_active_dates active using (date, user_pseudo_id, app_package_name)
    ),
    user_no_active_chain as (
        select
            date,
            user_pseudo_id,
            app_package_name,
            active_date,
            case
                when
                    active_date = false
                    and lag(active_date) over (
                        partition by user_pseudo_id, app_package_name order by date
                    )
                    is not false
                then 1
                else 0
            end as new_no_active_chain
        from user_all_dates_is_active
    ),
    user_no_active_days_count as (
        select
            date,
            user_pseudo_id,
            app_package_name,
            active_date,
            sum(new_no_active_chain) over (
                partition by user_pseudo_id, app_package_name order by date
            ) as chain_id
        from user_no_active_chain
    ),
    user_consecutive_no_active_count as (
        select
            date,
            user_pseudo_id,
            app_package_name,
            case
                when active_date = false
                then
                    row_number() over (
                        partition by user_pseudo_id, app_package_name, chain_id
                        order by date
                    )
                else 0
            end as consecutive_no_active_count
        from user_no_active_days_count
        order by user_pseudo_id, date
    ),
    user_churned as (
        select
            date,
            user_pseudo_id,
            app_package_name,
            case
                when consecutive_no_active_count = 28 then true else false
            end as churned
        from user_consecutive_no_active_count
    ),
    final as (
        select date, app_package_name, count(distinct user_pseudo_id) as churned_users
        from user_churned
        where churned is true
        group by date, app_package_name
        order by date, app_package_name
    )

select *
from final
