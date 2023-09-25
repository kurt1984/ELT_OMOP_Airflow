select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select visit_detail_id
from "dbt"."main"."visit_detail"
where visit_detail_id is null



      
    ) dbt_internal_test