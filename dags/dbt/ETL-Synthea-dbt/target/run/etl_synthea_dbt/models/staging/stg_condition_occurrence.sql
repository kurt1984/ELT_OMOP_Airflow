
  
  create view "dbt"."stg"."stg_condition_occurrence__dbt_tmp" as (
    SELECT
    p.person_id AS person_id,
    c.code,
    c.start,
    c.stop,
    fv.visit_occurrence_id_new
FROM "dbt"."source"."conditions" AS c
LEFT JOIN "dbt"."stg"."final_visit_ids" AS fv
    ON fv.encounter_id = c.encounter
INNER JOIN "dbt"."main"."person" AS p
    ON c.patient = p.person_source_value
  );
