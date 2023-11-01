import great_expectations as gx


context = gx.data_context.FileDataContext.create('.')

context.add_or_update_expectation_suite("my_expectation_suite")

# Give your Datasource a name
datasource_name = "patients_csv"
datasource = context.sources.add_pandas(datasource_name)

# Give your first Asset a name
asset_name = "patients"

# to use sample data uncomment next line
# path_to_data = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
path_to_data = "../dags/dbt/ETL-Synthea-dbt/data/synthea/patients.csv"
asset = datasource.add_csv_asset(asset_name, filepath_or_buffer=path_to_data)



# Build batch request
batch_request = asset.build_batch_request()

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="my_expectation_suite",
)

validator.expect_column_values_to_not_be_null(column="Id")


validator.save_expectation_suite()

checkpoint = context.add_or_update_checkpoint(
    name="my_checkpoint",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "my_expectation_suite",
        },
    ],
)