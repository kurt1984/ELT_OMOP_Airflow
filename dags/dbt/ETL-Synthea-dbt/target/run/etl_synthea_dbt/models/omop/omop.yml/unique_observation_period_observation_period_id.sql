select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    observation_period_id as unique_field,
    count(*) as n_records

from "dbt"."main"."observation_period"
where observation_period_id is not null
group by observation_period_id
having count(*) > 1



      
    ) dbt_internal_test