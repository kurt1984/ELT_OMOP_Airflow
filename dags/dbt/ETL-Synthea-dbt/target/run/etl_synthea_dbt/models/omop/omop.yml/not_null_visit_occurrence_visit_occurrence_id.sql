select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select visit_occurrence_id
from "dbt"."main"."visit_occurrence"
where visit_occurrence_id is null



      
    ) dbt_internal_test