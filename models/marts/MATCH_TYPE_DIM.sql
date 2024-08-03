


SELECT ROW_NUMBER() OVER (ORDER BY match_type) AS match_type_id, match_type 

FROM {{ ref('MATCH_DETAIL_CLEAN')}}

GROUP BY match_type
order by match_type