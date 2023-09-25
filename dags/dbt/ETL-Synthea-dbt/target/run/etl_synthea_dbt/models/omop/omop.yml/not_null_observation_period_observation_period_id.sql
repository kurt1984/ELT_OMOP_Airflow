select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select observation_period_id
from "dbt"."main"."observation_period"
where observation_period_id is null



      
    ) dbt_internal_test