
    
    

with child as (
    select visit_detail_id as from_field
    from "dbt"."main"."procedure_occurrence"
    where visit_detail_id is not null
),

parent as (
    select visit_detail_id as to_field
    from "dbt"."main"."visit_detail"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


