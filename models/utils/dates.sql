

{{
    config(
        materialized='table'
    )
}}


with series as (

    select * from generate_series(1, 1000) as i

),

dates as (

    select
        '2016-01-01'::timestamp + (interval '1 day' * i) as date

    from series

)

select
    date,
    to_char(date, 'YYYY-MM-DD') as nice_date

from dates
