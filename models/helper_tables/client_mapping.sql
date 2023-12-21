with
    client_mapping as (
        select
            app_package_name,
            client,
            flavour,
            product,
            sliide_percentage,
            partner_code
        from {{ source("helper_tables", "client_mapping") }}
        where do_not_use is false
    )

select *
from client_mapping
