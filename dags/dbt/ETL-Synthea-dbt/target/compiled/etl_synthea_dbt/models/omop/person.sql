WITH cte_concept_code AS
(
    SELECT
        *,

        CASE p.gender
            WHEN 'M' THEN p.gender
            WHEN 'F' THEN p.gender
            ELSE 'No matching concept'
        END AS gender_concept_code,
        CASE p.race
            WHEN 'white' THEN '5'
            WHEN 'black' THEN '3'
            WHEN 'asian' THEN '2'
            ELSE 'No matching concept'
        END AS race_concept_code,
        CASE p.ethnicity
            WHEN 'hispanic' THEN 'Hispanic'
            WHEN 'nonhispanic' THEN 'Not Hispanic'
            ELSE 'No matching concept'
        END AS ethnicity_concept_code
    FROM "dbt"."source"."patients" AS p
    --WHERE p.gender IS NOT NULL
)
SELECT
    ROW_NUMBER()OVER(ORDER BY person.id) AS person_id,
    gender_concept.concept_id AS gender_concept_id,
    DATE_PART('year', person.birthdate) AS year_of_birth,
    DATE_PART('month', person.birthdate) AS month_of_birth,
    DATE_PART('day', person.birthdate) AS day_of_birth,
    person.birthdate AS birth_datetime,
    race_concept.concept_id AS race_concept_id,
    ethnicity_concept.concept_id AS ethnicity_concept_id,
    cast(NULL as integer) AS location_id,
    cast(NULL as integer) AS provider_id,
    cast(NULL as integer) AS care_site_id,
    person.id AS person_source_value,
    person.gender AS gender_source_value,
    COALESCE(NULL, 0) AS gender_source_concept_id,
    person.race AS race_source_value,
    COALESCE(NULL, 0) AS race_source_concept_id,
    person.ethnicity AS ethnicity_source_value,
    COALESCE(NULL, 0) AS ethnicity_source_concept_id
FROM cte_concept_code AS person
LEFT JOIN "dbt"."cdm"."concept" AS Gender_concept
ON 
    (
        person.gender_concept_code = Gender_concept.concept_code
        AND Gender_concept.vocabulary_id = 'Gender'
    )
    OR
	(
		person.gender_concept_code = Gender_concept.concept_code
		AND Gender_concept.concept_code = 'No matching concept'
	)
LEFT JOIN "dbt"."cdm"."concept" AS Race_concept
ON 
    (
        person.race_concept_code = Race_concept.concept_code
        AND Race_concept.vocabulary_id = 'Race'
    )
    OR
	(
		person.race_concept_code = Race_concept.concept_code
		AND Race_concept.concept_code = 'No matching concept'
	)
LEFT JOIN "dbt"."cdm"."concept" AS Ethnicity_concept
ON 
    (
        person.ethnicity_concept_code = Ethnicity_concept.concept_code
        AND Ethnicity_concept.vocabulary_id = 'Ethnicity'
    )
    OR
	(
		person.ethnicity_concept_code = Ethnicity_concept.concept_code
		AND Ethnicity_concept.concept_code = 'No matching concept'
	)
--WHERE p.gender IS NOT NULL