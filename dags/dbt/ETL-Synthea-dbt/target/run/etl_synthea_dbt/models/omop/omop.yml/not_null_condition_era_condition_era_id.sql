select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select condition_era_id
from "dbt"."main"."condition_era"
where condition_era_id is null



      
    ) dbt_internal_test