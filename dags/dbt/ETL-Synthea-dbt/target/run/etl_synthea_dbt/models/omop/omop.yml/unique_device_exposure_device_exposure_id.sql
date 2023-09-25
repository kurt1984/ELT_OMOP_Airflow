select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    device_exposure_id as unique_field,
    count(*) as n_records

from "dbt"."main"."device_exposure"
where device_exposure_id is not null
group by device_exposure_id
having count(*) > 1



      
    ) dbt_internal_test