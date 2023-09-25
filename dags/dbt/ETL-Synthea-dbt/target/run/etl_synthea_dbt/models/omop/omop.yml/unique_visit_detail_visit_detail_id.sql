select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    visit_detail_id as unique_field,
    count(*) as n_records

from "dbt"."main"."visit_detail"
where visit_detail_id is not null
group by visit_detail_id
having count(*) > 1



      
    ) dbt_internal_test