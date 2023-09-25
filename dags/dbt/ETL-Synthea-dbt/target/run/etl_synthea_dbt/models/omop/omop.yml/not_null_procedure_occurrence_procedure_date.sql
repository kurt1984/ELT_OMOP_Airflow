select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select procedure_date
from "dbt"."main"."procedure_occurrence"
where procedure_date is null



      
    ) dbt_internal_test