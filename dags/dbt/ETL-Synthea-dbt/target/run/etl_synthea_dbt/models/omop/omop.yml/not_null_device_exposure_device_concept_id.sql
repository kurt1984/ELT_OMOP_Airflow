select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select device_concept_id
from "dbt"."main"."device_exposure"
where device_concept_id is null



      
    ) dbt_internal_test