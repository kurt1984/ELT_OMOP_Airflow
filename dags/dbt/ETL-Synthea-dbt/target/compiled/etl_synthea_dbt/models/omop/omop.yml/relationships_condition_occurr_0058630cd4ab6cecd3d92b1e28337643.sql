
    
    

with child as (
    select condition_status_source_value as from_field
    from "dbt"."main"."condition_occurrence"
    where condition_status_source_value is not null
),

parent as (
    select concept_id as to_field
    from "dbt"."cdm"."concept"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


