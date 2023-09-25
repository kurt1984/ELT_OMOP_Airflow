select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select measurement_concept_id
from "dbt"."main"."measurement"
where measurement_concept_id is null



      
    ) dbt_internal_test