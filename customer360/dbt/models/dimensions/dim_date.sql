{{ config(
    materialized='table',
    table_type='iceberg'
) }}

-- Standard date dimension spine covering past and upcoming operational horizons
with raw_dates as (
    select date_add('day', n, date '2024-01-01') as cal_date
    from (
        select (a.pos * 100 + b.pos * 10 + c.pos) as n
        from (values 0,1,2,3) as a(pos)
        cross join (values 0,1,2,3,4,5,6,7,8,9) as b(pos)
        cross join (values 0,1,2,3,4,5,6,7,8,9) as c(pos)
    )
    where date_add('day', n, date '2024-01-01') <= date '2027-12-31'
)

select
    cast(date_format(cal_date, '%Y%m%d') as bigint) as date_key,
    cal_date as calendar_date,
    year(cal_date) as calendar_year,
    quarter(cal_date) as calendar_quarter,
    month(cal_date) as calendar_month,
    date_format(cal_date, '%M') as month_name,
    day_of_month(cal_date) as day_of_month,
    day_of_week(cal_date) as day_of_week,
    date_format(cal_date, '%W') as day_name,
    case when day_of_week(cal_date) in (6, 7) then 1 else 0 end as is_weekend,
    case when day_of_week(cal_date) not in (6, 7) then 1 else 0 end as is_weekday
from raw_dates
