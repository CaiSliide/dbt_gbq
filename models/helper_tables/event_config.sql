with event_config as (
    select  
        product,
        event_name,
        is_session,
        is_active
    from 
        {{ source('helper_tables', 'event_config_new') }}
)