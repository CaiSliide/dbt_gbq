
with content_app_prod as (
    select  
        event_date,
        event_timestamp,
        event_name,
        event_params,
        user_pseudo_id
    from 
        {{ source('content-app_prod', 'events_*')}}
    where
        _TABLE_SUFFIX BETWEEN '20231201' AND '20231210'
)

select * from content_app_prod