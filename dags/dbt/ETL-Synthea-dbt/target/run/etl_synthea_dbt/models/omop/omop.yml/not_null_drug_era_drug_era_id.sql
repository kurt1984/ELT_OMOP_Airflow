select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select drug_era_id
from "dbt"."main"."drug_era"
where drug_era_id is null



      
    ) dbt_internal_test