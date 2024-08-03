
{{ config(materialized='table') }}


-- Define your start and end dates{{ config(materialized='table') }}

{% set start_date = '2023-10-12' %}
{% set end_date = '2023-11-10' %}


WITH RECURSIVE date_range AS (
    SELECT TO_DATE('{{ start_date }}') AS generated_date
    UNION ALL
    SELECT DATEADD(day, 1, generated_date)
    FROM date_range
    WHERE generated_date < TO_DATE('{{ end_date }}')
)

SELECT
    ROW_NUMBER() OVER (ORDER BY generated_date) AS DateID,
    generated_date AS FullDate,
    EXTRACT(DAY FROM generated_date) AS Day,
    EXTRACT(MONTH FROM generated_date) AS Month,
    EXTRACT(YEAR FROM generated_date) AS Year,
    EXTRACT(QUARTER FROM generated_date) AS Quarter,
    DAYOFWEEKISO(generated_date) AS DayOfWeek,
    EXTRACT(DAY FROM generated_date) AS DayOfMonth,
    DAYOFYEAR(generated_date) AS DayOfYear,
    DAYNAME(generated_date) AS DayOfWeekName,
    CASE WHEN DAYNAME(generated_date) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS IsWeekend
FROM date_range