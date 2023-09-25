select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    procedure_occurrence_id as unique_field,
    count(*) as n_records

from "dbt"."main"."procedure_occurrence"
where procedure_occurrence_id is not null
group by procedure_occurrence_id
having count(*) > 1



      
    ) dbt_internal_test