select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select observation_period_start_date
from "dbt"."main"."observation_period"
where observation_period_start_date is null



      
    ) dbt_internal_test