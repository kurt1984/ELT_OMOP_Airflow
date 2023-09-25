select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select condition_era_end_date
from "dbt"."main"."condition_era"
where condition_era_end_date is null



      
    ) dbt_internal_test