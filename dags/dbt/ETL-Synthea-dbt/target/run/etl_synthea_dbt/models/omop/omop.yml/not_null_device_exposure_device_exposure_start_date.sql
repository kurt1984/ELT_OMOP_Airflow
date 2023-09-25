select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select device_exposure_start_date
from "dbt"."main"."device_exposure"
where device_exposure_start_date is null



      
    ) dbt_internal_test