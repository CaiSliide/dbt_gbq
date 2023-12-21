with
    clients_not_in_client_mapping_events as (
        select
            app_package_name, client, flavour, product, sliide_percentage, partner_code
        from {{ ref("client_mapping") }}
        where
            app_package_name
            not in (select app_package_name from {{ ref("client_mapping_events") }})

    ),
    client_mapping_events as (
        select
            app_package_name,
            partner_code,
            product,
            client,
            flavour,
            version_from,
            version_to,
            lpad(split(version_from, '.')[offset(0)], 2, '0')
            || '.'
            || lpad(split(version_from, '.')[offset(1)], 2, '0')
            || '.'
            || lpad(
                split(version_from, '.')[offset(2)], 2, '0'
            ) as version_from_txt_short,
            lpad(split(version_to, '.')[offset(0)], 2, '0')
            || '.'
            || lpad(split(version_to, '.')[offset(1)], 2, '0')
            || '.'
            || lpad(split(version_to, '.')[offset(2)], 2, '0') as version_to_txt_short,
            lpad(split(version_from, '.')[offset(0)], 3, '0')
            || '.'
            || lpad(split(version_from, '.')[offset(1)], 3, '0')
            || '.'
            || lpad(
                split(version_from, '.')[offset(2)], 3, '0'
            ) as version_from_txt_long,
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
        from {{ ref("client_mapping_events") }}
    ),
    client_mapping_non_events as (
        select
            app_package_name,
            partner_code,
            product,
            client,
            flavour,
            '00.00.00' as version_from_txt_short,
            '99.99.99' as version_to_txt_short,
            '000.000.000' as version_from_txt_long,
            '999.999.999' as version_to_txt_long,
            safe_cast('0' as integer) as version_from_num_short,
            safe_cast('999999' as integer) as version_to_num_short,
            safe_cast('0' as integer) as version_from_num_long,
            safe_cast('999999999' as integer) as version_to_num_long
        from clients_not_in_client_mapping_events
    )

select
    app_package_name,
    partner_code,
    product,
    client,
    flavour,
    version_from_txt_short,
    version_to_txt_short,
    version_from_txt_long,
    version_to_txt_long,
    version_from_num_short,
    version_to_num_short,
    version_from_num_long,
    version_to_num_long
from client_mapping_events

union all

select
    app_package_name,
    partner_code,
    product,
    client,
    flavour,
    version_from_txt_short,
    version_to_txt_short,
    version_from_txt_long,
    version_to_txt_long,
    version_from_num_short,
    version_to_num_short,
    version_from_num_long,
    version_to_num_long
from client_mapping_non_events