
    
    

select
    drug_exposure_id as unique_field,
    count(*) as n_records

from "dbt"."main"."drug_exposure"
where drug_exposure_id is not null
group by drug_exposure_id
having count(*) > 1


