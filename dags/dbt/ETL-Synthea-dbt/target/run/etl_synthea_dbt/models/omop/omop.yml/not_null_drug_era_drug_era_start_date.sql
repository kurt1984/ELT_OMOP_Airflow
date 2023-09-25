select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select drug_era_start_date
from "dbt"."main"."drug_era"
where drug_era_start_date is null



      
    ) dbt_internal_test