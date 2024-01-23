
{% snapshot cai_test_scd %}

{{
    config(
      target_schema='snapshots',
      unique_key='event',
      strategy='check',
      check_cols='all',
    )
}}

select * from {{ source('sliide-grip-prod', 'cai_test_scd_source') }}

{% endsnapshot %}