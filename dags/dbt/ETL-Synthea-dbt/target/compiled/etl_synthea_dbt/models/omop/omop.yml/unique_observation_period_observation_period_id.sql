
    
    

select
    observation_period_id as unique_field,
    count(*) as n_records

from "dbt"."main"."observation_period"
where observation_period_id is not null
group by observation_period_id
having count(*) > 1


