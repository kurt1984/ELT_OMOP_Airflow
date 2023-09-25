select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select procedure_type_concept_id
from "dbt"."main"."procedure_occurrence"
where procedure_type_concept_id is null



      
    ) dbt_internal_test