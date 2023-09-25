
    
    

with child as (
    select value_as_concept_id as from_field
    from "dbt"."main"."observation"
    where value_as_concept_id is not null
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


