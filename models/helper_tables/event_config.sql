with event_config as (
    select  
        product,
        event_name,
        is_session,
        is_active
    from 
        {{ source('helper_tables', 'event_config_new') }}
)

select * from event_config
where   
    event_name not in (
        'c_dynamic_item_request',
        'c_dynamic_item_loadfinish',
        'c_lockscreen_loadfinish',
        'c_ad_loadfail',
        'session_start',
        'lockscreen_content_failed',
        'c_lockscreen_content_loadstart',
        'c_ad_request',
        'c_ad_loadfinish',
        'c_lockscreen_error',
        'lockscreen_content_load',
        'c_content_item_read_fail',
        'lockscreen_content_load_finished',
        'c_dynamic_item_loadfail',
        'boot_complete',
        'c_content_request',
        'unlock',
        'lockscreen_unlock'
    )