

SELECT ROW_NUMBER() OVER (ORDER BY a.venue,a.city) AS venue_id,
a.venue, a.city
FROM 
(select venue,
        case 
            when city is null 
            then 'NA'
            else city
        END as city
FROM {{ ref('MATCH_DETAIL_CLEAN')}}
group by 1,2 ) a