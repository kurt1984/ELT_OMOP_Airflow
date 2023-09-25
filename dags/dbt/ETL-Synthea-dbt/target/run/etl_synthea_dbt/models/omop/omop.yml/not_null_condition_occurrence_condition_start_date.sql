select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select condition_start_date
from "dbt"."main"."condition_occurrence"
where condition_start_date is null



      
    ) dbt_internal_test