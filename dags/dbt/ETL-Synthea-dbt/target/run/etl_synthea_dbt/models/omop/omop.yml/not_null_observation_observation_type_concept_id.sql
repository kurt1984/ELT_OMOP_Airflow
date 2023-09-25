select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select observation_type_concept_id
from "dbt"."main"."observation"
where observation_type_concept_id is null



      
    ) dbt_internal_test