with
    primary_mapping as (
        select
            app_package_name,
            partner_code,
            product,
            client,
            flavour,
            version_from,
            version_to
        from {{ ref("client_mapping_events") }}
    ),
    fallback_mapping as (
        select
            app_package_name,
            partner_code,
            product,
            client,
            flavour,
            '00.00.00' version_from,
            '99.99.99' version_to
        from {{ ref("client_mapping") }}
        where
            app_package_name
            not in (select distinct app_package_name from primary_mapping)
    ),
    union_all_clients as (
        select *
        from primary_mapping
        union all
        select *
        from fallback_mapping
    )

select
    app_package_name,
    partner_code,
    product,
    client,
    flavour,
    lpad(split(version_from, '.')[offset(0)], 2, '0')
    || '.'
    || lpad(split(version_from, '.')[offset(1)], 2, '0')
    || '.'
    || lpad(split(version_from, '.')[offset(2)], 2, '0') as version_from_txt_short,
    lpad(split(version_to, '.')[offset(0)], 2, '0')
    || '.'
    || lpad(split(version_to, '.')[offset(1)], 2, '0')
    || '.'
    || lpad(split(version_to, '.')[offset(2)], 2, '0') as version_to_txt_short,
    lpad(split(version_from, '.')[offset(0)], 3, '0')
    || '.'
    || lpad(split(version_from, '.')[offset(1)], 3, '0')
    || '.'
    || lpad(split(version_from, '.')[offset(2)], 3, '0') as version_from_txt_long,
    lpad(split(version_to, '.')[offset(0)], 3, '0')
    || '.'
    || lpad(split(version_to, '.')[offset(1)], 3, '0')
    || '.'
    || lpad(split(version_to, '.')[offset(2)], 3, '0') as version_to_txt_long,
    safe_cast(
        lpad(split(version_from, '.')[offset(0)], 2, '0')
        || lpad(split(version_from, '.')[offset(1)], 2, '0')
        || lpad(split(version_from, '.')[offset(2)], 2, '0') as integer
    ) as version_from_num_short,
    safe_cast(
        lpad(split(version_to, '.')[offset(0)], 2, '0')
        || lpad(split(version_to, '.')[offset(1)], 2, '0')
        || lpad(split(version_to, '.')[offset(2)], 2, '0') as integer
    ) as version_to_num_short,
    safe_cast(
        lpad(split(version_from, '.')[offset(0)], 3, '0')
        || lpad(split(version_from, '.')[offset(1)], 3, '0')
        || lpad(split(version_from, '.')[offset(2)], 3, '0') as integer
    ) as version_from_num_long,
    safe_cast(
        lpad(split(version_to, '.')[offset(0)], 3, '0')
        || lpad(split(version_to, '.')[offset(1)], 3, '0')
        || lpad(split(version_to, '.')[offset(2)], 3, '0') as integer
    ) as version_to_num_long
from union_all_clients
