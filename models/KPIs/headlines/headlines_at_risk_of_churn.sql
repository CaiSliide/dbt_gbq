with
    base_0_to_28_day_users as (
        select
            user_pseudo_id,
            app_family_name,
            app_package_name,
            app_version_string,
            device_model,
            device_os_version,
            max(session_start_date) as max_date
        from {{ ref("headlines_sessions") }}
        where
            is_active_session is true
            and session_start_date
            between date_sub(current_date(), interval 27 day) and current_date()
        group by
            user_pseudo_id,
            app_family_name,
            app_package_name,
            app_version_string,
            device_model,
            device_os_version
    )

-- By Flavour breakdown
select
    current_date() as date,
    app_family_name as product,
    app_package_name as flavour,
    'total' as version,
    'total' as device_model,
    'total' as operating_system,
    count(distinct user_pseudo_id) as non_churned_users,
    {% set at_risk_churn_intervals = [
        {"name": "3_to_6_days_ago", "start": 3, "end": 6},
        {"name": "7_to_13_days_ago", "start": 7, "end": 13},
        {"name": "14_to_20_days_ago", "start": 14, "end": 20},
        {"name": "21_to_27_days_ago", "start": 21, "end": 27},
    ] %}
    {% for at_risk_churn_interval in at_risk_churn_intervals %}
        count(
            distinct case
                when
                    max_date
                    between date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["end"] }} day
                    ) and date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["start"] }} day
                    )
                then user_pseudo_id
            end
        ) as last_activity_{{ at_risk_churn_interval["name"] }},
        count(
            distinct case
                when
                    max_date
                    between date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["end"] }} day
                    ) and date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["start"] }} day
                    )
                then user_pseudo_id
            end
        )
        / count(distinct user_pseudo_id)
        * 100 as last_activity_{{ at_risk_churn_interval["name"] }}_percent
        {% if not loop.last %}, {% endif %}
    {% endfor %}

from base_0_to_28_day_users
group by date, product, flavour

union all
-- -- By Flavour & Version breakdown
select
    current_date() as date,
    app_family_name as product,
    app_package_name as flavour,
    app_version_string as version,
    'total' as device_model,
    'total' as operating_system,
    count(distinct user_pseudo_id) as non_churned_users,
    {% set at_risk_churn_intervals = [
        {"name": "3_to_6_days_ago", "start": 3, "end": 6},
        {"name": "7_to_13_days_ago", "start": 7, "end": 13},
        {"name": "14_to_20_days_ago", "start": 14, "end": 20},
        {"name": "21_to_27_days_ago", "start": 21, "end": 27},
    ] %}
    {% for at_risk_churn_interval in at_risk_churn_intervals %}
        count(
            distinct case
                when
                    max_date
                    between date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["end"] }} day
                    ) and date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["start"] }} day
                    )
                then user_pseudo_id
            end
        ) as last_activity_{{ at_risk_churn_interval["name"] }},
        count(
            distinct case
                when
                    max_date
                    between date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["end"] }} day
                    ) and date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["start"] }} day
                    )
                then user_pseudo_id
            end
        )
        / count(distinct user_pseudo_id)
        * 100 as last_activity_{{ at_risk_churn_interval["name"] }}_percent
        {% if not loop.last %}, {% endif %}
    {% endfor %}

from base_0_to_28_day_users
group by date, product, flavour, version

union all
-- -- By Flavour & Version & Device breakdown
select
    current_date() as date,
    app_family_name as product,
    app_package_name as flavour,
    app_version_string as version,
    device_model,
    'total' as operating_system,
    count(distinct user_pseudo_id) as non_churned_users,
    {% set at_risk_churn_intervals = [
        {"name": "3_to_6_days_ago", "start": 3, "end": 6},
        {"name": "7_to_13_days_ago", "start": 7, "end": 13},
        {"name": "14_to_20_days_ago", "start": 14, "end": 20},
        {"name": "21_to_27_days_ago", "start": 21, "end": 27},
    ] %}
    {% for at_risk_churn_interval in at_risk_churn_intervals %}
        count(
            distinct case
                when
                    max_date
                    between date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["end"] }} day
                    ) and date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["start"] }} day
                    )
                then user_pseudo_id
            end
        ) as last_activity_{{ at_risk_churn_interval["name"] }},
        count(
            distinct case
                when
                    max_date
                    between date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["end"] }} day
                    ) and date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["start"] }} day
                    )
                then user_pseudo_id
            end
        )
        / count(distinct user_pseudo_id)
        * 100 as last_activity_{{ at_risk_churn_interval["name"] }}_percent
        {% if not loop.last %}, {% endif %}
    {% endfor %}

from base_0_to_28_day_users
group by date, product, flavour, version, device_model

union all
-- -- By Flavour & Version & OS breakdown
select
    current_date() as date,
    app_family_name as product,
    app_package_name as flavour,
    app_version_string as version,
    'total' as device_model,
    device_os_version as operating_system,
    count(distinct user_pseudo_id) as non_churned_users,
    {% set at_risk_churn_intervals = [
        {"name": "3_to_6_days_ago", "start": 3, "end": 6},
        {"name": "7_to_13_days_ago", "start": 7, "end": 13},
        {"name": "14_to_20_days_ago", "start": 14, "end": 20},
        {"name": "21_to_27_days_ago", "start": 21, "end": 27},
    ] %}
    {% for at_risk_churn_interval in at_risk_churn_intervals %}
        count(
            distinct case
                when
                    max_date
                    between date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["end"] }} day
                    ) and date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["start"] }} day
                    )
                then user_pseudo_id
            end
        ) as last_activity_{{ at_risk_churn_interval["name"] }},
        count(
            distinct case
                when
                    max_date
                    between date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["end"] }} day
                    ) and date_sub(
                        current_date(),
                        interval {{ at_risk_churn_interval["start"] }} day
                    )
                then user_pseudo_id
            end
        )
        / count(distinct user_pseudo_id)
        * 100 as last_activity_{{ at_risk_churn_interval["name"] }}_percent
        {% if not loop.last %}, {% endif %}
    {% endfor %}

from base_0_to_28_day_users
group by date, product, flavour, version, operating_system
