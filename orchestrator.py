import os
import json
import snowflake.connector
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    RunStatusSensorContext,
    run_status_sensor,
    DagsterRunStatus,
    AssetSelection,
    DefaultSensorStatus
)
from dagster_dbt import dbt_cloud_resource, load_assets_from_dbt_cloud_job

# 1. Configure the connection to dbt Cloud
# These will be pulled from 'Environment Variables' in the Dagster Plus UI
dbt_cloud_connection = dbt_cloud_resource.configured(
    {
        "auth_token": os.getenv("DBT_CLOUD_API_TOKEN"),
        "account_id": int(os.getenv("DBT_CLOUD_ACCOUNT_ID")),
        "dbt_cloud_host": os.getenv("DBT_CLOUD_HOST"), 
    }
)

# 2. Pull assets from dbt Cloud Job
ecommerce_dbt_assets = load_assets_from_dbt_cloud_job(
    dbt_cloud=dbt_cloud_connection,
    job_id=int(os.getenv("DBT_JOB_ID"))
)

# 3. Snowflake Logging Logic
def write_run_to_snowflake(context: RunStatusSensorContext, status: str, error_msg: dict = None):
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database="SANDBOX",
            schema="METRICS"
        )
        cursor = conn.cursor()
        
        run_id = context.dagster_run.run_id
        job_name = context.dagster_run.job_name
        
        stats = context.instance.get_run_stats(run_id)
        start_time = stats.start_time
        end_time = stats.end_time
        
        error_json = json.dumps(error_msg) if error_msg else "{}"

        sql = """
            INSERT INTO DAGSTER_JOB_RUNS 
            (RUN_ID, JOB_NAME, STATUS, START_TIME, END_TIME, ERROR_MESSAGE)
            VALUES (%s, %s, %s, TO_TIMESTAMP_NTZ(%s), TO_TIMESTAMP_NTZ(%s), PARSE_JSON(%s))
        """
        cursor.execute(sql, (run_id, job_name, status, start_time, end_time, error_json))
        conn.commit()
    finally:
        if 'conn' in locals(): conn.close()

# 4. Auto-enabled Sensors
@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, default_status=DefaultSensorStatus.RUNNING)
def log_success_to_snowflake(context: RunStatusSensorContext):
    write_run_to_snowflake(context, status="SUCCESS")

@run_status_sensor(run_status=DagsterRunStatus.FAILURE, default_status=DefaultSensorStatus.RUNNING)
def log_failure_to_snowflake(context: RunStatusSensorContext):
    error_data = {"error": context.failure_event.message} if context.failure_event else None
    write_run_to_snowflake(context, status="FAILURE", error_msg=error_data)

# 5. Define Job
run_ecommerce_pipeline = define_asset_job(
    name="trigger_ecommerce_dbt_cloud_job",
    selection=AssetSelection.all()
)

# 6. Final Definitions
defs = Definitions(
    assets=[ecommerce_dbt_assets],
    jobs=[run_ecommerce_pipeline],
    sensors=[log_success_to_snowflake, log_failure_to_snowflake],
)