
    
    

select
    visit_detail_id as unique_field,
    count(*) as n_records

from "dbt"."main"."visit_detail"
where visit_detail_id is not null
group by visit_detail_id
having count(*) > 1


