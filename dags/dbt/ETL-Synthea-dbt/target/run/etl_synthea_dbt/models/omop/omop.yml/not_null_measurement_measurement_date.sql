select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select measurement_date
from "dbt"."main"."measurement"
where measurement_date is null



      
    ) dbt_internal_test