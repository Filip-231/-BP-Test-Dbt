from __future__ import division, print_function, unicode_literals
import json
from datetime import datetime
from os import environ as env
import argparse


def load_manifest():
    local_filepath = f"../dbt/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data


def clean_name(name):
    return name.replace("$", "__")


def clean_task_name(task_name):
    return task_name.replace(".", "_")


def make_dbt_task(node_name, subtask_id, model_path, is_test):
    dbt_task = f"""PythonOperator(
        task_id="{node_name}",
        python_callable=run_dbt_task,
        provide_context=True,
        op_kwargs={{"subtask_id": "{subtask_id}", "model_path": "{model_path}", "is_test": {is_test}}}
    )
"""
    return dbt_task


def generate_imports(file_string=""):
    file_string += """from datetime import datetime
import logging
import sys
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
    """
    return file_string


def generate_default_configs(file_string="", project_name=""):
    file_string += f"""
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
tag_list = ["DBT", "{project_name}"]
default_args = {{
    'start_date': datetime(2021, 1, 1),
    'owner': 'Admin'
}}
DOCKER_IMAGE = "dbt-jaffle-shop:latest"
GLOBAL_CLI_FLAGS = "--no-write-json --no-use-colors" #prevents DBT from writing a new manifest.json file and remove colors from logs
"""
    return file_string


def generate_dbt_task_executor(file_string=""):
    file_string += f"""def run_dbt_task(subtask_id="", model_path="", is_test=False, **kwargs):
    if kwargs.get('dag_run').conf:
        DBT_TARGET = kwargs.get('dag_run').conf.get("DBT_TARGET", "dev")
        FULL_REFRESH = kwargs.get('dag_run').conf.get("FULL_REFRESH", False)
    else:
        DBT_TARGET = "dev"
        FULL_REFRESH = False
    print(f"DBT_TARGET -> {{DBT_TARGET}}\\nFULL_REFRESH -> {{FULL_REFRESH}}")
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
            command=f"dbt {{GLOBAL_CLI_FLAGS}} {{dbt_command}} --profiles-dir profile --target {{DBT_TARGET}} --models {{model_path}}",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            tty=True
        )
    dbt_task.execute(dict())
    """
    return file_string


def generate_dag_setup(file_string="", project_name="", schedule=None):
    if "None" not in schedule:
        schedule = f"'{schedule}'"
    file_string += f"""
with DAG('DBT-{project_name}', schedule_interval={schedule}, default_args=default_args, tags=tag_list, catchup=False) as dag:
"""
    return file_string


def generate_tasks(file_string="", manifest={}):
    dbt_tasks = {}
    dbt_sources_tasks = {}
    for node in manifest["nodes"].keys():
        if node.split(".")[0] == "model":
            # Define values for model/test name/path
            node_values = manifest["nodes"][node]
            model_path = '.'.join(node_values["fqn"][1:])
            node_name = clean_name(f"{node_values['schema']}.{node_values['name']}")
            node_test_name = f"test.{node_name}"
            # Define values for dependent sources name/path and create task if unexistant
            for dependency_node in node_values["depends_on"]["nodes"]:
                dependency_node_type = dependency_node.split(".")[0]
                if dependency_node_type == "source":
                    dependency_node_values = manifest["sources"][dependency_node]
                    dependency_model_path = '.'.join(dependency_node_values["fqn"][1:])
                    dependency_node_name = clean_name(f"source.{dependency_model_path}")
                    dependency_node_test_name = f"test.{dependency_node_name}"
                    if dependency_node_test_name not in dbt_sources_tasks.keys():
                        for child in manifest["child_map"][dependency_node]:
                            if "test" in child:
                                dbt_sources_tasks[dependency_node_test_name] = make_dbt_task(dependency_node_test_name,
                                                                                             f'docker_{dependency_node_test_name}',
                                                                                             f"source:{dependency_model_path}",
                                                                                             True)
                                break
            dbt_tasks[node_name] = make_dbt_task(node_name, f'docker_{node_name}', model_path, False)
            for child in manifest["child_map"][node]:
                if "test" in child:
                    dbt_tasks[node_test_name] = make_dbt_task(node_test_name, f'docker_{node_test_name}', model_path,
                                                              True)
                    break
    for source_task_name, source_task_value in dbt_sources_tasks.items():
        file_string += f"\n    {clean_task_name(source_task_name)} = {source_task_value}"
    for task_name, task_value in dbt_tasks.items():
        file_string += f"\n    {clean_task_name(task_name)} = {task_value}"
    return file_string


def generate_task_dependency(file_string="", manifest={}):
    for node in manifest["nodes"].keys():
        if node.split(".")[0] == "model":
            node_values = manifest["nodes"][node]
            node_name = clean_name(f"{node_values['schema']}.{node_values['name']}")
            if any("test." in node_dependencies for node_dependencies in manifest["child_map"][node]):
                node_test_name = f"test.{node_name}"
                dependency = f"\n    {clean_task_name(node_name)} >> {clean_task_name(node_test_name)}"
                if dependency not in file_string:
                    file_string += dependency
            # Set test sources -> run model -> test model -> run next model (test before next model approach only if test exists)
            for upstream_node in node_values["depends_on"]["nodes"]:
                dependency = None
                if "model" in upstream_node:
                    upstream_node_values = manifest["nodes"][upstream_node]
                    upstream_node_name = clean_name(f"{upstream_node_values['schema']}.{upstream_node_values['name']}")
                    upstream_node_test_name = f"test.{upstream_node_name}"

                    if any("test." in upstream_dependencies for upstream_dependencies in
                           manifest["child_map"][upstream_node]):
                        # if there's a test for the upstream node, set current model after it
                        dependency = f"\n    {clean_task_name(upstream_node_test_name)} >> {clean_task_name(node_name)}"
                    else:
                        # else, set model after the upstream node
                        dependency = f"\n    {clean_task_name(upstream_node_name)} >> {clean_task_name(node_name)}"
                elif "source" in upstream_node and any("test." in upstream_dependencies for upstream_dependencies in
                                                       manifest["child_map"][upstream_node]):
                    # if there's a test for the upstream node and it is also a source, set current model after it
                    upstream_node_values = manifest["sources"][upstream_node]
                    upstream_model_path = '.'.join(upstream_node_values["fqn"][1:])
                    upstream_node_name = clean_name(f"source.{upstream_model_path}")
                    upstream_node_test_name = f"test.{upstream_node_name}"
                    dependency = f"\n    {clean_task_name(upstream_node_test_name)} >> {clean_task_name(node_name)}"
                if dependency and dependency not in file_string:
                    file_string += dependency
    return file_string


def main(schedule):
    manifest = load_manifest()
    generated_dag_script = generate_imports(f"# DAG CREATED BY PYTHON SCRIPT -> {datetime.now()}\n")
    generated_dag_script = generate_default_configs(file_string=generated_dag_script, project_name="dbt-dag")
    generated_dag_script = generate_dbt_task_executor(file_string=generated_dag_script)
    generated_dag_script = generate_dag_setup(file_string=generated_dag_script, project_name="dbt-dag",
                                              schedule=schedule)
    generated_dag_script = generate_tasks(file_string=generated_dag_script, manifest=manifest)
    generated_dag_script = generate_task_dependency(file_string=generated_dag_script, manifest=manifest)
    with open("dbt_dag.py", "w") as f:
        f.write(generated_dag_script)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-schedule", help="""schedules the DAG with provided CRON schedule (required)""", required=True,
                        default=None, action='store', dest='schedule')
    args = parser.parse_args()

    main(args.schedule)