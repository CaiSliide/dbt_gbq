with session_parameters as (
    select  
        product,
        session_time,
        churn_time
    from 
        {{ source('helper_tables', 'session_parameters')}}
)

select * from session_parameters