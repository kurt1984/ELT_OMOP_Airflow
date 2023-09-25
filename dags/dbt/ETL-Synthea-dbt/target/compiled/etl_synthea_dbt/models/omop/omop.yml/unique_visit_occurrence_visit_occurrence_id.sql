
    
    

select
    visit_occurrence_id as unique_field,
    count(*) as n_records

from "dbt"."main"."visit_occurrence"
where visit_occurrence_id is not null
group by visit_occurrence_id
having count(*) > 1


