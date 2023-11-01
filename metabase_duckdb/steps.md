
from [AlexR2D2/metabase_duckdb_driver: Metabase DuckDB Driver shipped as 3rd party plugin](https://github.com//AlexR2D2/metabase_duckdb_driver)

```
docker build . --tag metaduck:latest

cd ../dags/dbt
docker run --name metaduck -d -p 80:3000 -m 2GB -e MB_PLUGINS_DIR=/home/plugins -v $(pwd):/container/directory metaduck 
```


to view the logs as your Open Source Metabase initializes, run:

```
docker logs -f metaduck

```

Next, in the settings page of DuckDB of Metabase Web UI you could set your DB file name like this

/container/directory/ETL-Synthea-dbt/dbt.duckdb


For now, we recommend that you load the database file in a supported version of DuckDB, and use the EXPORT DATABASE command followed by IMPORT DATABASE on the current version of DuckDB.

