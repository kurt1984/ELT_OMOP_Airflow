select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select year_of_birth
from "dbt"."main"."person"
where year_of_birth is null



      
    ) dbt_internal_test