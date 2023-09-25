select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select visit_start_date
from "dbt"."main"."visit_occurrence"
where visit_start_date is null



      
    ) dbt_internal_test