with primary_mapping as (
    select 
        app_package_name
        , product
        , version_from_num_short version_from
        , version_to_num_short version_to 
    from 
        `sliide-grip-prod.mdm_helper_tables.client_mapping_events_version`
)
, fallback_mapping as (
    select 
        app_package_name
        , product
        , 0 version_from
        , 999999 version_to 
    from 
        `sliide-grip-prod.mdm_helper_tables.client_mapping`
    where 
        app_package_name not in (select distinct app_package_name from primary_mapping)
)
select * from primary_mapping
union all
select * from fallback_mapping