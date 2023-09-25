
    
    

select
    condition_era_id as unique_field,
    count(*) as n_records

from "dbt"."main"."condition_era"
where condition_era_id is not null
group by condition_era_id
having count(*) > 1


