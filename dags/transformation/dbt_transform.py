import os

import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, ExecutionMode, \
    SourceRenderingBehavior
from cosmos.profiles import AthenaAccessKeyProfileMapping

dbt_project_path = "/opt/airflow/dags/dbt/dbt_project"
profile_ = AthenaAccessKeyProfileMapping(
    conn_id="aws_default",
    profile_args={
        "schema": "datalake",
        "region_name": "us-east-1",
        "database": "awsdatacatalog",
        "s3_data_dir": "s3://victorouttes-landing/dw/",
        "s3_staging_dir": "s3://victorouttes-landing/athena/",
    }
)
profile_config = ProfileConfig(
    profile_mapping=profile_,
    profile_name="dbt_project",
    target_name="dev",
)

@dag(start_date=pendulum.yesterday(), schedule=None, catchup=False, tags=["dbt"])
def generate_dag():
    DbtTaskGroup(
        group_id="bronze_to_silver_gold",
        project_config=ProjectConfig(
            dbt_project_path
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_env/bin/dbt"
        ),
        profile_config=profile_config,
        render_config=RenderConfig(
            source_rendering_behavior=SourceRenderingBehavior.ALL,
        ),
        operator_args={
            "install_deps": True,
        }
    )

generate_dag()
