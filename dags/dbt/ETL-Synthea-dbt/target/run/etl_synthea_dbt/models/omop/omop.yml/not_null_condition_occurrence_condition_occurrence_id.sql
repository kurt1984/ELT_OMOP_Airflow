select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select condition_occurrence_id
from "dbt"."main"."condition_occurrence"
where condition_occurrence_id is null



      
    ) dbt_internal_test