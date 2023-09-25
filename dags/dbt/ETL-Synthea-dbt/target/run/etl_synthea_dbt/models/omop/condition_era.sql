
  
    
    

    create  table
      "dbt"."main"."condition_era__dbt_tmp"
  
    as (
      SELECT
    condition_era_id,
    person_id,
    condition_concept_id,
    condition_era_start_date,
    condition_era_end_date,
    condition_occurrence_count 
FROM "dbt"."stg"."tmp_ce"
    );
  
  