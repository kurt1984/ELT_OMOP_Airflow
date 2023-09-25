
    
    

select
    drug_era_id as unique_field,
    count(*) as n_records

from "dbt"."main"."drug_era"
where drug_era_id is not null
group by drug_era_id
having count(*) > 1


