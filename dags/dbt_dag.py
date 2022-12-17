# DAG CREATED BY PYTHON SCRIPT -> 2022-12-04 19:05:24.646862
from datetime import datetime
import logging
import sys
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
    
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
tag_list = ["DBT", "dbt-dag"]
default_args = {
    'start_date': datetime(2021, 1, 1),
    'owner': 'Admin'
}
DOCKER_IMAGE = "dbt-jaffle-shop:latest"
GLOBAL_CLI_FLAGS = "--no-write-json --no-use-colors" #prevents DBT from writing a new manifest.json file and remove colors from logs
def run_dbt_task(subtask_id="", model_path="", is_test=False, **kwargs):
    if kwargs.get('dag_run').conf:
        DBT_TARGET = kwargs.get('dag_run').conf.get("DBT_TARGET", "dev")
        FULL_REFRESH = kwargs.get('dag_run').conf.get("FULL_REFRESH", False)
    else:
        DBT_TARGET = "dev"
        FULL_REFRESH = False
    print(f"DBT_TARGET -> {DBT_TARGET}\nFULL_REFRESH -> {FULL_REFRESH}")
    dbt_command = "run"
    if is_test:
        dbt_command = "test"
    elif FULL_REFRESH:
        dbt_command = "run --full-refresh"
    dbt_task = DockerOperator(
            task_id=subtask_id,
            image=DOCKER_IMAGE,
            api_version='auto',
            auto_remove=True,
            command=f"dbt {GLOBAL_CLI_FLAGS} {dbt_command} --profiles-dir profile --target {DBT_TARGET} --models {model_path}",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            tty=True
        )
    dbt_task.execute(dict())
    
with DAG('DBT-dbt-dag', schedule_interval='*/5 * * * *', default_args=default_args, tags=tag_list, catchup=False) as dag:

    test_source_staging_initializations_DBT_FILIP_pipeline_initialization = PythonOperator(
        task_id="test.source.staging.initializations.DBT_FILIP.pipeline_initialization",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_test.source.staging.initializations.DBT_FILIP.pipeline_initialization", "model_path": "source:staging.initializations.DBT_FILIP.pipeline_initialization", "is_test": True}
    )

    test_source_staging_executions_DBT_FILIP_pipeline_execution = PythonOperator(
        task_id="test.source.staging.executions.DBT_FILIP.pipeline_execution",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_test.source.staging.executions.DBT_FILIP.pipeline_execution", "model_path": "source:staging.executions.DBT_FILIP.pipeline_execution", "is_test": True}
    )

    DBT_FILIP_stg_initialization = PythonOperator(
        task_id="DBT_FILIP.stg_initialization",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_DBT_FILIP.stg_initialization", "model_path": "staging.initializations.stg_initialization", "is_test": False}
    )

    DBT_FILIP_stg_executions = PythonOperator(
        task_id="DBT_FILIP.stg_executions",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_DBT_FILIP.stg_executions", "model_path": "staging.executions.stg_executions", "is_test": False}
    )

    DBT_FILIP_executions_view = PythonOperator(
        task_id="DBT_FILIP.executions_view",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_DBT_FILIP.executions_view", "model_path": "marts.executions_view", "is_test": False}
    )

    DBT_FILIP_mrt_feed_instance_status = PythonOperator(
        task_id="DBT_FILIP.mrt_feed_instance_status",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_DBT_FILIP.mrt_feed_instance_status", "model_path": "marts.FeedInstanceStatus.mrt_feed_instance_status", "is_test": False}
    )

    DBT_FILIP_mrt_execution_status = PythonOperator(
        task_id="DBT_FILIP.mrt_execution_status",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_DBT_FILIP.mrt_execution_status", "model_path": "marts.ExecutionStatus.mrt_execution_status", "is_test": False}
    )

    DBT_FILIP_int_events = PythonOperator(
        task_id="DBT_FILIP.int_events",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={"subtask_id": "docker_DBT_FILIP.int_events", "model_path": "intermediate.events.int_events", "is_test": False}
    )

    test_source_staging_initializations_DBT_FILIP_pipeline_initialization >> DBT_FILIP_stg_initialization
    test_source_staging_executions_DBT_FILIP_pipeline_execution >> DBT_FILIP_stg_executions
    test_source_staging_initializations_DBT_FILIP_pipeline_initialization >> DBT_FILIP_executions_view
    test_source_staging_executions_DBT_FILIP_pipeline_execution >> DBT_FILIP_executions_view
    DBT_FILIP_int_events >> DBT_FILIP_mrt_feed_instance_status
    DBT_FILIP_int_events >> DBT_FILIP_mrt_execution_status
    test_source_staging_executions_DBT_FILIP_pipeline_execution >> DBT_FILIP_int_events
    test_source_staging_initializations_DBT_FILIP_pipeline_initialization >> DBT_FILIP_int_events
    DBT_FILIP_stg_initialization >> DBT_FILIP_int_events