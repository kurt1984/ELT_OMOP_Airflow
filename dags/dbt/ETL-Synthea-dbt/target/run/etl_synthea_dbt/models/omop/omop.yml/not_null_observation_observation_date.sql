select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select observation_date
from "dbt"."main"."observation"
where observation_date is null



      
    ) dbt_internal_test