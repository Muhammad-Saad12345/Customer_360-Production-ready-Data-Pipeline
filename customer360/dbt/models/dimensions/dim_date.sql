{{ config(
    materialized='table',
    table_type='iceberg'
) }}

-- Calendar spine 2016-01-01 .. 2030-12-31 (covers the Olist history and the
-- current years). The previous digit cross-join produced only 400 days, so the
-- spine ended in February 2025.
with raw_dates as (
    select cal_date
    from unnest(sequence(date '2016-01-01', date '2030-12-31', interval '1' day)) as t(cal_date)
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
