CREATE SCHEMA cdm;
CREATE SCHEMA source;
CREATE SCHEMA vocab;
CREATE SCHEMA stg;



CREATE TABLE cdm.vocabulary(vocabulary_id VARCHAR, vocabulary_name VARCHAR, vocabulary_reference VARCHAR, vocabulary_version VARCHAR, vocabulary_concept_id BIGINT);
CREATE TABLE cdm.concept_ancestor(ancestor_concept_id BIGINT, descendant_concept_id BIGINT, min_levels_of_separation BIGINT, max_levels_of_separation BIGINT);
CREATE TABLE cdm.concept_relationship(concept_id_1 BIGINT, concept_id_2 BIGINT, relationship_id VARCHAR, valid_start_date BIGINT, valid_end_date BIGINT, invalid_reason VARCHAR);
CREATE TABLE cdm.concept(concept_id BIGINT, concept_name VARCHAR, domain_id VARCHAR, vocabulary_id VARCHAR, concept_class_id VARCHAR, standard_concept VARCHAR, concept_code VARCHAR, valid_start_date BIGINT, valid_end_date BIGINT, invalid_reason VARCHAR);
CREATE TABLE source.immunizations(DATE TIMESTAMP, PATIENT VARCHAR, ENCOUNTER VARCHAR, CODE VARCHAR, DESCRIPTION VARCHAR, BASE_COST DOUBLE);
CREATE TABLE source.medications("START" TIMESTAMP, STOP TIMESTAMP, PATIENT VARCHAR, PAYER VARCHAR, ENCOUNTER VARCHAR, CODE BIGINT, DESCRIPTION VARCHAR, BASE_COST DOUBLE, PAYER_COVERAGE DOUBLE, DISPENSES BIGINT, TOTALCOST DOUBLE, REASONCODE BIGINT, REASONDESCRIPTION VARCHAR);
CREATE TABLE source.devices("START" TIMESTAMP, STOP TIMESTAMP, PATIENT VARCHAR, ENCOUNTER VARCHAR, CODE BIGINT, DESCRIPTION VARCHAR, UDI VARCHAR);
CREATE TABLE source.procedures("START" TIMESTAMP, STOP TIMESTAMP, PATIENT VARCHAR, ENCOUNTER VARCHAR, CODE BIGINT, DESCRIPTION VARCHAR, BASE_COST DOUBLE, REASONCODE BIGINT, REASONDESCRIPTION VARCHAR);
CREATE TABLE source.observations(DATE TIMESTAMP, PATIENT VARCHAR, ENCOUNTER VARCHAR, CATEGORY VARCHAR, CODE VARCHAR, DESCRIPTION VARCHAR, "VALUE" VARCHAR, UNITS VARCHAR, "TYPE" VARCHAR);
CREATE TABLE source.allergies("START" DATE, STOP VARCHAR, PATIENT VARCHAR, ENCOUNTER VARCHAR, CODE BIGINT, "SYSTEM" VARCHAR, DESCRIPTION VARCHAR, "TYPE" VARCHAR, CATEGORY VARCHAR, REACTION1 BIGINT, DESCRIPTION1 VARCHAR, SEVERITY1 VARCHAR, REACTION2 BIGINT, DESCRIPTION2 VARCHAR, SEVERITY2 VARCHAR);
CREATE TABLE source.conditions("START" DATE, STOP DATE, PATIENT VARCHAR, ENCOUNTER VARCHAR, CODE BIGINT, DESCRIPTION VARCHAR);
CREATE TABLE source.providers(Id VARCHAR, ORGANIZATION VARCHAR, "NAME" VARCHAR, GENDER VARCHAR, SPECIALITY VARCHAR, ADDRESS VARCHAR, CITY VARCHAR, STATE VARCHAR, ZIP VARCHAR, LAT DOUBLE, LON DOUBLE, UTILIZATION BIGINT);
CREATE TABLE source.encounters(Id VARCHAR, "START" TIMESTAMP, STOP TIMESTAMP, PATIENT VARCHAR, ORGANIZATION VARCHAR, PROVIDER VARCHAR, PAYER VARCHAR, ENCOUNTERCLASS VARCHAR, CODE BIGINT, DESCRIPTION VARCHAR, BASE_ENCOUNTER_COST DOUBLE, TOTAL_CLAIM_COST DOUBLE, PAYER_COVERAGE DOUBLE, REASONCODE BIGINT, REASONDESCRIPTION VARCHAR);
CREATE TABLE source.patients(Id VARCHAR, BIRTHDATE DATE, DEATHDATE DATE, SSN VARCHAR, DRIVERS VARCHAR, PASSPORT VARCHAR, PREFIX VARCHAR, "FIRST" VARCHAR, "LAST" VARCHAR, SUFFIX VARCHAR, MAIDEN VARCHAR, MARITAL VARCHAR, RACE VARCHAR, ETHNICITY VARCHAR, GENDER VARCHAR, BIRTHPLACE VARCHAR, ADDRESS VARCHAR, CITY VARCHAR, STATE VARCHAR, COUNTY VARCHAR, ZIP VARCHAR, LAT DOUBLE, LON DOUBLE, HEALTHCARE_EXPENSES DOUBLE, HEALTHCARE_COVERAGE DOUBLE);
CREATE TABLE stg.source_to_standard_vocab_map(source_code VARCHAR, source_concept_id BIGINT, source_code_description VARCHAR, source_vocabulary_id VARCHAR, source_domain_id VARCHAR, source_concept_class_id VARCHAR, source_valid_start_date BIGINT, source_valid_end_date BIGINT, source_invalid_reason VARCHAR, target_concept_id BIGINT, target_concept_name VARCHAR, target_vocabulary_id VARCHAR, target_domain_id VARCHAR, target_concept_class_id VARCHAR, target_invalid_reason VARCHAR, target_standard_concept VARCHAR);
CREATE TABLE stg.source_to_source_vocab_map(source_code VARCHAR, source_concept_id BIGINT, source_code_description VARCHAR, source_vocabulary_id VARCHAR, source_domain_id VARCHAR, source_concept_class_id VARCHAR, source_valid_start_date BIGINT, source_valid_end_date BIGINT, source_invalid_reason VARCHAR, target_concept_id BIGINT, target_concept_name VARCHAR, target_vocabulary_id VARCHAR, target_domain_id VARCHAR, target_concept_class_id VARCHAR, target_invalid_reason VARCHAR, target_standard_concept VARCHAR);
CREATE TABLE visit_occurrence(visit_occurrence_id BIGINT, person_id BIGINT, visit_concept_id BIGINT, visit_start_date TIMESTAMP, visit_start_datetime TIMESTAMP, visit_end_date TIMESTAMP, visit_end_datetime TIMESTAMP, visit_type_concept_id INTEGER, provider_id BIGINT, care_site_id INTEGER, visit_source_value VARCHAR, visit_source_concept_id INTEGER, admitting_source_concept_id INTEGER, admitting_source_value INTEGER, discharge_to_concept_id INTEGER, discharge_to_source_value INTEGER, preceding_visit_occurrence_id BIGINT);
CREATE TABLE observation(observation_id BIGINT, person_id BIGINT, observation_concept_id BIGINT, observation_date TIMESTAMP, observation_datetime TIMESTAMP, observation_type_concept_id INTEGER, value_as_number FLOAT, value_as_string VARCHAR, value_as_concept_id INTEGER, qualifier_concept_id INTEGER, unit_concept_id INTEGER, provider_id BIGINT, visit_occurrence_id BIGINT, visit_detail_id BIGINT, observation_source_value VARCHAR, observation_source_concept_id BIGINT, unit_source_value VARCHAR, qualifier_source_value VARCHAR, value_source_value INTEGER, observation_event_id INTEGER, obs_event_field_concept_id INTEGER);
CREATE TABLE drug_exposure(drug_exposure_id BIGINT, person_id BIGINT, drug_concept_id BIGINT, drug_exposure_start_date TIMESTAMP, drug_exposure_start_datetime TIMESTAMP, drug_exposure_end_date TIMESTAMP, drug_exposure_end_datetime TIMESTAMP, verbatim_end_date TIMESTAMP, drug_type_concept_id INTEGER, stop_reason VARCHAR, refills INTEGER, quantity INTEGER, days_supply BIGINT, sig VARCHAR, route_concept_id INTEGER, lot_number INTEGER, provider_id BIGINT, visit_occurrence_id BIGINT, visit_detail_id BIGINT, drug_source_value VARCHAR, drug_source_concept_id BIGINT, route_source_value VARCHAR, dose_unit_source_value VARCHAR);
CREATE TABLE observation_period(observation_period_id BIGINT, person_id BIGINT, observation_period_start_date TIMESTAMP, observation_period_end_date TIMESTAMP, period_type_concept_id INTEGER);
CREATE TABLE provider(provider_id BIGINT, provider_name VARCHAR, npi VARCHAR, dea VARCHAR, specialty_concept_id INTEGER, care_site_id INTEGER, year_of_birth INTEGER, gender_concept_id INTEGER, provider_source_value VARCHAR, specialty_source_value VARCHAR, specialty_source_concept_id INTEGER, gender_source_value VARCHAR, gender_source_concept_id INTEGER);
CREATE TABLE cdm_source(cdm_source_name VARCHAR, cdm_source_abbreviation VARCHAR, cdm_holder VARCHAR, source_description VARCHAR, source_documentation_reference VARCHAR, cdm_etl_reference VARCHAR, source_release_date TIMESTAMP WITH TIME ZONE, cdm_release_date TIMESTAMP WITH TIME ZONE, cdm_version VARCHAR, vocabulary_version VARCHAR);
CREATE TABLE person(person_id BIGINT, gender_concept_id BIGINT, year_of_birth BIGINT, month_of_birth BIGINT, day_of_birth BIGINT, birth_datetime DATE, race_concept_id BIGINT, ethnicity_concept_id BIGINT, location_id INTEGER, provider_id INTEGER, care_site_id INTEGER, person_source_value VARCHAR, gender_source_value VARCHAR, gender_source_concept_id INTEGER, race_source_value VARCHAR, race_source_concept_id INTEGER, ethnicity_source_value VARCHAR, ethnicity_source_concept_id INTEGER);
CREATE TABLE death(person_id BIGINT, death_date TIMESTAMP, death_datetime TIMESTAMP, death_type_concept_id INTEGER, cause_concept_id BIGINT, cause_source_value BIGINT, cause_source_concept_id BIGINT);
CREATE TABLE device_exposure(device_exposure_id BIGINT, person_id BIGINT, device_concept_id BIGINT, device_exposure_start_date TIMESTAMP, device_exposure_start_datetime TIMESTAMP, device_exposure_end_date TIMESTAMP, device_exposure_end_datetime TIMESTAMP, device_type_concept_id INTEGER, unique_device_id VARCHAR, quantity INTEGER, provider_id BIGINT, visit_occurrence_id BIGINT, visit_detail_id BIGINT, device_source_value BIGINT, device_source_concept_id BIGINT, unit_concept_id INTEGER, unit_source_value INTEGER, unit_source_concept_id INTEGER);
CREATE TABLE measurement(measurement_id BIGINT, person_id BIGINT, measurement_concept_id BIGINT, measurement_date TIMESTAMP, measurement_datetime TIMESTAMP, measurement_time TIMESTAMP, measurement_type_concept_id INTEGER, operator_concept_id INTEGER, value_as_number FLOAT, value_as_concept_id BIGINT, unit_concept_id BIGINT, range_low FLOAT, range_high FLOAT, provider_id BIGINT, visit_occurrence_id BIGINT, visit_detail_id BIGINT, measurement_source_value VARCHAR, measurement_source_concept_id BIGINT, unit_source_value VARCHAR, value_source_value VARCHAR);
CREATE TABLE procedure_occurrence(procedure_occurrence_id BIGINT, person_id BIGINT, procedure_concept_id BIGINT, procedure_date TIMESTAMP, procedure_datetime TIMESTAMP, procedure_type_concept_id INTEGER, modifier_concept_id INTEGER, quantity INTEGER, provider_id BIGINT, visit_occurrence_id BIGINT, visit_detail_id BIGINT, procedure_source_value BIGINT, procedure_source_concept_id BIGINT, modifier_source_value INTEGER);
CREATE TABLE visit_detail(visit_detail_id BIGINT, person_id BIGINT, visit_detail_concept_id INTEGER, visit_detail_start_date TIMESTAMP, visit_detail_start_datetime TIMESTAMP, visit_detail_end_date TIMESTAMP, visit_detail_end_datetime TIMESTAMP, visit_detail_type_concept_id INTEGER, provider_id BIGINT, care_site_id INTEGER, admitting_source_concept_id INTEGER, discharge_to_concept_id INTEGER, preceding_visit_detail_id BIGINT, visit_detail_source_value VARCHAR, visit_detail_source_concept_id INTEGER, admitting_source_value INTEGER, discharge_to_source_value INTEGER, parent_visit_detail_id INTEGER, visit_occurrence_id BIGINT);
CREATE TABLE condition_occurrence(condition_occurrence_id BIGINT, person_id BIGINT, condition_concept_id BIGINT, condition_start_date DATE, condition_start_datetime DATE, condition_end_date DATE, condition_end_datetime DATE, condition_type_concept_id INTEGER, stop_reason VARCHAR, provider_id INTEGER, visit_occurrence_id BIGINT, visit_detail_id BIGINT, condition_source_value BIGINT, condition_source_concept_id INTEGER, condition_status_source_value INTEGER, condition_status_concept_id INTEGER);
CREATE TABLE drug_era(drug_era_id BIGINT, person_id BIGINT, drug_concept_id BIGINT, drug_era_start_date TIMESTAMP, drug_era_end_date TIMESTAMP, drug_exposure_count HUGEINT, gap_days HUGEINT);
CREATE TABLE condition_era(condition_era_id BIGINT, person_id BIGINT, condition_concept_id BIGINT, condition_era_start_date DATE, condition_era_end_date DATE, condition_occurrence_count BIGINT);

/* {"app": "dbt", "dbt_version": "1.6.3", "profile_name": "etl_synthea_dbt", "target_name": "omop", "node_id": "model.etl_synthea_dbt.tmp_ce"} */

  
  create view "dbt"."stg"."tmp_ce__dbt_tmp" as (
    -- This script is taken from here:
-- https://github.com/OHDSI/ETL-CMS/blob/master/SQL/create_CDMv5_condition_era.sql
--

-- if object_id('tempdb..#tmp_ce', 'U') is not null drop table #tmp_ce;

WITH cteConditionTarget (
    condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date
) AS (
    SELECT
        co.condition_occurrence_id,
        co.person_id,
        co.condition_concept_id,
        co.condition_start_date,
        COALESCE(NULLIF(co.condition_end_date, NULL), co.condition_start_date + INTERVAL '1 day')
    FROM "dbt"."main"."condition_occurrence" AS co
/* Depending on the needs of your data, you can put more filters on to your code. We assign 0 to our unmapped condition_concept_id's,
* and since we don't want different conditions put in the same era, we put in the filter below.
*/
---WHERE condition_concept_id != 0
),

--------------------------------------------------------------------------------------------------------------
cteEndDates (person_id, condition_concept_id, end_date) AS -- the magic
(
    SELECT
        e.person_id,
        e.condition_concept_id,
        e.event_date - 30 * INTERVAL '1 day' AS end_date -- unpad the end date
    FROM
        (
            SELECT
                person_id,
                condition_concept_id,
                event_date,
                event_type,
                MAX(
                    start_ordinal
                ) OVER (
                    PARTITION BY
                        person_id, condition_concept_id
                    ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING
                ) AS start_ordinal,
                -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with
                ROW_NUMBER() OVER (
                    PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type
                ) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
            FROM
                (
                    -- select the start dates, assigning a row number to each
                    SELECT
                        person_id,
                        condition_concept_id,
                        condition_start_date AS event_date,
                        -1 AS event_type,
                        ROW_NUMBER() OVER (PARTITION BY person_id,
                            condition_concept_id ORDER BY condition_start_date) AS start_ordinal
                    FROM cteConditionTarget

                    UNION ALL

                    -- pad the end dates by 30 to allow a grace period for overlapping ranges.
                    SELECT
                        person_id,
                        condition_concept_id,
                        condition_end_date + 30 * INTERVAL '1 day',
                        1 AS event_type,
                        NULL
                    FROM cteConditionTarget
                ) AS RAWDATA
        ) AS e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),

--------------------------------------------------------------------------------------------------------------
cteConditionEnds (person_id, condition_concept_id, condition_start_date, era_end_date) AS (
    SELECT
        c.person_id,
        c.condition_concept_id,
        c.condition_start_date,
        MIN(e.end_date) AS era_end_date
    FROM cteConditionTarget AS c
    INNER JOIN
        cteEndDates AS e ON
            c.person_id = e.person_id AND c.condition_concept_id = e.condition_concept_id AND e.end_date >= c.condition_start_date
    GROUP BY
        c.condition_occurrence_id,
        c.person_id,
        c.condition_concept_id,
        c.condition_start_date
)
--------------------------------------------------------------------------------------------------------------


SELECT 
    ROW_NUMBER()OVER(ORDER BY person_id) AS condition_era_id,
    person_id,
    condition_concept_id,
    MIN(condition_start_date) AS condition_era_start_date,
    era_end_date AS condition_era_end_date,
    COUNT(*) AS condition_occurrence_count
FROM cteConditionEnds
GROUP BY person_id, condition_concept_id, era_end_date
  );

;
/* {"app": "dbt", "dbt_version": "1.6.3", "profile_name": "etl_synthea_dbt", "target_name": "omop", "node_id": "model.etl_synthea_dbt.tmp_de"} */

  
  create view "dbt"."stg"."tmp_de__dbt_tmp" as (
    -- Code taken from:
-- https://github.com/OHDSI/ETL-CMS/blob/master/SQL/create_CDMv5_drug_era_non_stockpile.sql


-- if object_id('tempdb..#tmp_de', 'U') is not null drop table #tmp_de;

WITH
ctePreDrugTarget(
    drug_exposure_id, person_id, ingredient_concept_id, drug_exposure_start_date, days_supply, drug_exposure_end_date
) AS (
    -- Normalize DRUG_EXPOSURE_END_DATE to either the existing drug exposure end date, or add days supply, or add 1 day to the start date
    SELECT
        d.drug_exposure_id,
        d.person_id,
        c.concept_id AS ingredient_concept_id,
        d.drug_exposure_start_date AS drug_exposure_start_date,
        d.days_supply AS days_supply,
        COALESCE(
            ---NULLIF returns NULL if both values are the same, otherwise it returns the first parameter
            NULLIF(drug_exposure_end_date, NULL),
            ---If drug_exposure_end_date != NULL, return drug_exposure_end_date, otherwise go to next case
            NULLIF(drug_exposure_start_date + days_supply * INTERVAL '1 day', drug_exposure_start_date),
            ---If days_supply != NULL or 0, return drug_exposure_start_date + days_supply, otherwise go to next case
            drug_exposure_start_date + INTERVAL '1 day'
        ---Add 1 day to the drug_exposure_start_date since there is no end_date or INTERVAL for the days_supply
        ) AS drug_exposure_end_date
    FROM "dbt"."main"."drug_exposure" AS d
    INNER JOIN "dbt"."cdm"."concept_ancestor" AS ca ON ca.descendant_concept_id = d.drug_concept_id
    INNER JOIN "dbt"."cdm"."concept" AS c ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 'RxNorm' ---8 selects RxNorm from the vocabulary_id
        AND c.concept_class_id = 'Ingredient'
        AND d.drug_concept_id != 0 ---Our unmapped drug_concept_id's are set to 0, so we don't want different drugs wrapped up in the same era
        AND COALESCE(d.days_supply, 0) >= 0
---We have cases where days_supply is negative, and this can set the end_date before the start_date, which we don't want. So we're just looking over those rows. This is a data-quality issue.
),

cteSubExposureEndDates (
    person_id, ingredient_concept_id, end_date
) AS --- A preliminary sorting that groups all of the overlapping exposures into one exposure so that we don't double-count non-gap-days
(
    SELECT
        e.person_id,
        e.ingredient_concept_id,
        e.event_date AS end_date
    FROM
        (
            SELECT
                person_id,
                ingredient_concept_id,
                event_date,
                event_type,
                MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id
                    ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
                -- this pulls the current START down from the prior rows so that the NULLs
                -- from the END DATES will contain a value we can compare with
                ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id
                    ORDER BY event_date, event_type) AS overall_ord
            -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
            FROM (
                -- select the start dates, assigning a row number to each
                SELECT
                    person_id,
                    ingredient_concept_id,
                    drug_exposure_start_date AS event_date,
                    -1 AS event_type,
                    ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id
                        ORDER BY drug_exposure_start_date) AS start_ordinal
                FROM ctePreDrugTarget

                UNION ALL

                SELECT
                    person_id,
                    ingredient_concept_id,
                    drug_exposure_end_date,
                    1 AS event_type,
                    NULL
                FROM ctePreDrugTarget
            ) AS RAWDATA
        ) AS e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),

cteDrugExposureEnds (person_id, drug_concept_id, drug_exposure_start_date, drug_sub_exposure_end_date) AS (
    SELECT
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date,
        MIN(e.end_date) AS drug_sub_exposure_end_date
    FROM ctePreDrugTarget AS dt
    INNER JOIN
        cteSubExposureEndDates AS e ON
            dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
    GROUP BY
        dt.drug_exposure_id,
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date
),

--------------------------------------------------------------------------------------------------------------
cteSubExposures(
    row_number,
    person_id,
    drug_concept_id,
    drug_sub_exposure_start_date,
    drug_sub_exposure_end_date,
    drug_exposure_count
) AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY person_id, drug_concept_id, drug_sub_exposure_end_date ORDER BY person_id),
        person_id,
        drug_concept_id,
        MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date,
        drug_sub_exposure_end_date,
        COUNT(*) AS drug_exposure_count
    FROM cteDrugExposureEnds
    GROUP BY person_id, drug_concept_id, drug_sub_exposure_end_date
--ORDER BY person_id, drug_concept_id
),

--------------------------------------------------------------------------------------------------------------
/*Everything above grouped exposures into sub_exposures if there was overlap between exposures.
*So there was no persistence window. Now we can add the persistence window to calculate eras.
*/
--------------------------------------------------------------------------------------------------------------
cteFinalTarget(
    row_number,
    person_id,
    ingredient_concept_id,
    drug_sub_exposure_start_date,
    drug_sub_exposure_end_date,
    drug_exposure_count,
    days_exposed
) AS (
    SELECT
        row_number,
        person_id,
        drug_concept_id,
        drug_sub_exposure_start_date,
        drug_sub_exposure_end_date,
        drug_exposure_count,
        drug_sub_exposure_end_date::DATE - drug_sub_exposure_start_date::DATE AS days_exposed
    FROM cteSubExposures
),

--------------------------------------------------------------------------------------------------------------
cteEndDates (person_id, ingredient_concept_id, end_date) AS -- the magic
(
    SELECT
        e.person_id,
        e.ingredient_concept_id,
        e.event_date - 30 * INTERVAL '1 day' AS end_date -- unpad the end date
    FROM
        (
            SELECT
                person_id,
                ingredient_concept_id,
                event_date,
                event_type,
                MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id
                    ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
                -- this pulls the current START down from the prior rows so that the NULLs
                -- from the END DATES will contain a value we can compare with
                ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id
                    ORDER BY event_date, event_type) AS overall_ord
            -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
            FROM (
                -- select the start dates, assigning a row number to each
                SELECT
                    person_id,
                    ingredient_concept_id,
                    drug_sub_exposure_start_date AS event_date,
                    -1 AS event_type,
                    ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id
                        ORDER BY drug_sub_exposure_start_date) AS start_ordinal
                FROM cteFinalTarget

                UNION ALL

                -- pad the end dates by 30 to allow a grace period for overlapping ranges.
                SELECT
                    person_id,
                    ingredient_concept_id,
                    drug_sub_exposure_end_date + 30 * INTERVAL '1 day',
                    1 AS event_type,
                    NULL
                FROM cteFinalTarget
            ) AS RAWDATA
        ) AS e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0

),

cteDrugEraEnds (
    person_id, drug_concept_id, drug_sub_exposure_start_date, drug_era_end_date, drug_exposure_count, days_exposed
) AS (
    SELECT
        ft.person_id,
        ft.ingredient_concept_id,
        ft.drug_sub_exposure_start_date,
        MIN(e.end_date) AS era_end_date,
        drug_exposure_count,
        days_exposed
    FROM cteFinalTarget AS ft
    INNER JOIN
        cteEndDates AS e ON
            ft.person_id = e.person_id AND ft.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= ft.drug_sub_exposure_start_date
    GROUP BY
        ft.person_id,
        ft.ingredient_concept_id,
        ft.drug_sub_exposure_start_date,
        drug_exposure_count,
        days_exposed
)

SELECT
    ROW_NUMBER()OVER(ORDER BY person_id) AS drug_era_id,
    person_id,
    drug_concept_id,
    MIN(drug_sub_exposure_start_date) AS drug_era_start_date,
    drug_era_end_date,
    SUM(drug_exposure_count) AS drug_exposure_count,
    (drug_era_end_date::DATE - MIN(drug_sub_exposure_start_date)::DATE) - SUM(days_exposed) AS gap_days
FROM cteDrugEraEnds
GROUP BY person_id, drug_concept_id, drug_era_end_date
  );

;
/* {"app": "dbt", "dbt_version": "1.6.3", "profile_name": "etl_synthea_dbt", "target_name": "omop", "node_id": "model.etl_synthea_dbt.stg_condition_occurrence"} */

  
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

;
/* {"app": "dbt", "dbt_version": "1.6.3", "profile_name": "etl_synthea_dbt", "target_name": "omop", "node_id": "model.etl_synthea_dbt.final_visit_ids"} */

  
  create view "dbt"."stg"."final_visit_ids__dbt_tmp" as (
    SELECT
    encounter_id,
    VISIT_OCCURRENCE_ID_NEW
FROM(
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY encounter_id ORDER BY PRIORITY) AS RN
    FROM (
        SELECT
            *,
            CASE
                WHEN encounterclass IN ('emergency', 'urgent')
                    THEN (
                        CASE
                            WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
                                THEN 1
                            WHEN VISIT_TYPE IN ('emergency', 'urgent') AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
                                THEN 2
                            ELSE 99
                        END
                    )
                WHEN encounterclass IN ('ambulatory', 'wellness', 'outpatient')
                    THEN (
                        CASE
                            WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
                                THEN 1
                            WHEN
                                VISIT_TYPE IN (
                                    'ambulatory', 'wellness', 'outpatient'
                                ) AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
                                THEN 2
                            ELSE 99
                        END
                    )
                WHEN encounterclass = 'inpatient' AND VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
                    THEN 1
                ELSE 99
            END AS PRIORITY
        FROM "dbt"."stg"."assign_all_visit_ids"
    ) AS T1
) AS T2
WHERE RN = 1
  );

;
/* {"app": "dbt", "dbt_version": "1.6.3", "profile_name": "etl_synthea_dbt", "target_name": "omop", "node_id": "model.etl_synthea_dbt.all_visits"} */

  
  create view "dbt"."stg"."all_visits__dbt_tmp" as (
    /* Inpatient visits */
/* Collapse IP claim lines with <=1 day between them into one visit */

WITH CTE_END_DATES AS (
    SELECT
        patient,
        encounterclass,
        EVENT_DATE - INTERVAL '1 day' AS END_DATE
    FROM (
        SELECT
            patient,
            encounterclass,
            EVENT_DATE,
            EVENT_TYPE,
            max(
                START_ORDINAL
            ) OVER (
                PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE ROWS UNBOUNDED PRECEDING
            ) AS START_ORDINAL,
            row_number() OVER (PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE) AS OVERALL_ORD
        FROM (
            SELECT
                patient,
                encounterclass,
                start AS EVENT_DATE,
                -1 AS EVENT_TYPE,
                row_number() OVER (PARTITION BY patient, encounterclass ORDER BY start, stop) AS START_ORDINAL
            FROM "dbt"."source"."encounters"
            WHERE encounterclass = 'inpatient'
            UNION ALL
            SELECT
                patient,
                encounterclass,
                stop + INTERVAL '1 day' AS EVENT_DATE,
                1 AS EVENT_TYPE,
                NULL
            FROM "dbt"."source"."encounters"
            WHERE encounterclass = 'inpatient'
        ) AS RAWDATA
    ) AS E
    WHERE (2 * E.START_ORDINAL - E.OVERALL_ORD = 0)
),

CTE_VISIT_ENDS AS (
    SELECT
        min(V.id) AS encounter_id,
        V.patient,
        V.encounterclass,
        V.start AS VISIT_START_DATE,
        min(E.END_DATE) AS VISIT_END_DATE
    FROM "dbt"."source"."encounters" AS V
    INNER JOIN CTE_END_DATES AS E
        ON V.patient = E.patient
            AND V.encounterclass = E.encounterclass
            AND E.END_DATE >= V.start
    GROUP BY V.patient, V.encounterclass, V.start
),

IP_VISITS AS (
    SELECT
        T2.encounter_id,
        T2.patient,
        T2.encounterclass,
        T2.VISIT_START_DATE,
        T2.VISIT_END_DATE
        
    FROM (
            SELECT
                encounter_id,
                patient,
                encounterclass,
                min(VISIT_START_DATE) AS VISIT_START_DATE,
                VISIT_END_DATE
            FROM CTE_VISIT_ENDS
            GROUP BY encounter_id, patient, encounterclass, VISIT_END_DATE
        ) AS T2
),


/* Emergency visits */
/* collapse ER claim lines with no days between them into one visit */

ER_VISITS AS (
    SELECT
        T2.encounter_id,
        T2.patient,
        T2.encounterclass,
        T2.VISIT_START_DATE,
        T2.VISIT_END_DATE
        
    FROM (
            SELECT
                min(encounter_id) AS encounter_id,
                patient,
                encounterclass,
                VISIT_START_DATE,
                max(VISIT_END_DATE) AS VISIT_END_DATE
            FROM (
                    SELECT
                        CL1.id AS encounter_id,
                        CL1.patient,
                        CL1.encounterclass,
                        CL1.start AS VISIT_START_DATE,
                        CL2.stop AS VISIT_END_DATE
                    FROM "dbt"."source"."encounters" AS CL1
                    INNER JOIN "dbt"."source"."encounters" AS CL2
                        ON CL1.patient = CL2.patient
                            AND CL1.start = CL2.start
                            AND CL1.encounterclass = CL2.encounterclass
                    WHERE CL1.encounterclass IN ('emergency', 'urgent')
                ) AS T1
            GROUP BY patient, encounterclass, VISIT_START_DATE
        ) AS T2
),


/* Outpatient visits */

CTE_VISITS_DISTINCT AS (
    SELECT
        min(id) AS encounter_id,
        patient,
        encounterclass,
        start AS VISIT_START_DATE,
        stop AS VISIT_END_DATE
    FROM "dbt"."source"."encounters"
    WHERE encounterclass IN ('ambulatory', 'wellness', 'outpatient')
    GROUP BY patient, encounterclass, start, stop
),

OP_VISITS AS (
    SELECT
        min(encounter_id) AS encounter_id,
        patient,
        encounterclass,
        VISIT_START_DATE,
        max(VISIT_END_DATE) AS VISIT_END_DATE
        
    FROM CTE_VISITS_DISTINCT
    GROUP BY patient, encounterclass, VISIT_START_DATE
)

/* All visits */

SELECT
    *,
    row_number()OVER(ORDER BY patient) AS visit_occurrence_id
FROM
    (
        SELECT * FROM IP_VISITS
    
        UNION ALL
        SELECT * FROM ER_VISITS
    
        UNION ALL
        SELECT * FROM OP_VISITS
    ) AS T1
  );

;
/* {"app": "dbt", "dbt_version": "1.6.3", "profile_name": "etl_synthea_dbt", "target_name": "omop", "node_id": "model.etl_synthea_dbt.assign_all_visit_ids"} */

  
  create view "dbt"."stg"."assign_all_visit_ids__dbt_tmp" as (
    /*Assign VISIT_OCCURRENCE_ID to all encounters*/

SELECT
    E.id AS encounter_id,
    E.patient AS person_source_value,
    E.start AS date_service,
    E.stop AS date_service_end,
    E.encounterclass,
    AV.encounterclass AS VISIT_TYPE,
    AV.VISIT_START_DATE,
    AV.VISIT_END_DATE,
    AV.VISIT_OCCURRENCE_ID,
    CASE
        WHEN E.encounterclass = 'inpatient' AND AV.encounterclass = 'inpatient'
            THEN VISIT_OCCURRENCE_ID
        WHEN E.encounterclass IN ('emergency', 'urgent')
            THEN (
                CASE
                    WHEN AV.encounterclass = 'inpatient' AND E.start > AV.VISIT_START_DATE
                        THEN VISIT_OCCURRENCE_ID
                    WHEN AV.encounterclass IN ('emergency', 'urgent') AND E.start = AV.VISIT_START_DATE
                        THEN VISIT_OCCURRENCE_ID
                END
            )
        WHEN E.encounterclass IN ('ambulatory', 'wellness', 'outpatient')
            THEN (
                CASE
                    WHEN AV.encounterclass = 'inpatient' AND E.start >= AV.VISIT_START_DATE
                        THEN VISIT_OCCURRENCE_ID
                    WHEN AV.encounterclass IN ('ambulatory', 'wellness', 'outpatient')
                        THEN VISIT_OCCURRENCE_ID
                END
            )
    END AS VISIT_OCCURRENCE_ID_NEW
FROM "dbt"."source"."encounters" AS E
INNER JOIN "dbt"."stg"."all_visits" AS AV
    ON E.patient = AV.patient
        AND E.start >= AV.VISIT_START_DATE
        AND E.start <= AV.VISIT_END_DATE
  );

;



