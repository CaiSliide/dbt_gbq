with client_mapping_events as (
    select  
        app_package_name,
        partner_code,
        product,
        client,
        flavour, 
        do_not_use, 
        version_from, 
        version_to, 
        tstmp_added, 
        tstmp_updated
    from 
        {{ source('helper_tables', 'client_mapping_events') }}
)

select * from client_mapping_events