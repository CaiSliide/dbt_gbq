with
    base as (
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
    )

select
    app_package_name,
    max(partner_code) as partner_code,
    max(product) as product,
    max(client) as client,
    max(flavour) as flavour,
    version_from_txt_short,
    version_to_txt_short,
    version_from_txt_long,
    version_to_txt_long,
    version_from_num_short,
    version_to_num_short,
    version_from_num_long,
    version_to_num_long
from base
group by 1, 6, 7, 8, 9, 10, 11, 12, 13
