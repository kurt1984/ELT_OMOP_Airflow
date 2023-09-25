select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select period_type_concept_id
from "dbt"."main"."observation_period"
where period_type_concept_id is null



      
    ) dbt_internal_test