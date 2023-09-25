
#%%
import duckdb
con = duckdb.connect('dbt.duckdb')

synthea_tables = ["patients", "encounters", "providers", "conditions", "allergies", "observations", "procedures", "devices", "medications", "immunizations"]
vocab_tables = ["concept", "concept_relationship", "concept_ancestor", "vocabulary"]

for table in synthea_tables:
    con.sql(f'create table IF NOT EXISTS source.{table} as FROM "data/synthea/{table}.csv"')

for table in vocab_tables:
    con.sql(f'create table IF NOT EXISTS cdm.{table} as FROM "data/vocab/{table.upper()}.csv"')

con.table("source.encounters").show()

con.close()

# %%
