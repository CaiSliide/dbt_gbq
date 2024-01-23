with
    user_base as (
        select last_seen_app_info_id as app_package_name, user_pseudo_id, activation_date
        from {{ ref("headlines_users") }}
        -- where activation_date between '2023-11-01' and '2023-11-30'
        where activation_date >= '2023-09-01'
    ),
    session_base as (
        select distinct
            user_base.app_package_name,
            user_pseudo_id,
            activation_date,
            {% for i in range(1, 31) %}
                case
                    when session_start_date = activation_date + {{ i }}
                    then true
                    else false
                end as day_{{ i }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ ref("headlines_sessions") }}
        inner join user_base using (app_package_name, user_pseudo_id)
        where
            -- session_start_date between '2023-11-01' and '2023-12-30'
            is_active_session is true
    ),
    agg_retention as (
        select
            app_package_name,
            activation_date,
            count(distinct user_pseudo_id) as total_user_cohort_count,
            {% for i in range(1, 31) %}
                count(
                    distinct case when day_{{ i }} is true then user_pseudo_id end
                ) as day_{{ i }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from session_base
        group by app_package_name, activation_date
    )

select
    app_package_name,
    activation_date,
    {% for i in range(1, 31) %}
        day_{{ i }} / total_user_cohort_count as retention_day_{{ i }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
from agg_retention
