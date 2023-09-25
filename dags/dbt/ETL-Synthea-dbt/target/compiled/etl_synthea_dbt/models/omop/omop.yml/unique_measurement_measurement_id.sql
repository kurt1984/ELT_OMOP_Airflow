
    
    

select
    measurement_id as unique_field,
    count(*) as n_records

from "dbt"."main"."measurement"
where measurement_id is not null
group by measurement_id
having count(*) > 1


