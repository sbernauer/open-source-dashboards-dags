import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.trino.hooks.trino import TrinoHook

@dag(
    dag_id="process-github-orgs",
    schedule_interval="*/5 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=3),
)
def ProcessGithubOrgs():
    @task()
    def create_github_schema():
        TrinoOperator(
            task_id="create_github_schema",
            sql="""
                CREATE SCHEMA IF NOT EXISTS lakehouse.github WITH (
                    location = 's3a://open-source-dashboards/github'
                )""",
        )

    @task()
    def create_github_orgs_table():
        TrinoOperator(
            task_id="create_github_orgs_table",
            sql="""
                CREATE TABLE IF NOT EXISTS lakehouse.github.orgs (
                    id bigint,
                    node_id varchar,
                    login varchar,
                    url varchar,
                    repos_url varchar,
                    events_url varchar,
                    hooks_url varchar,
                    issues_url varchar,
                    members_url varchar,
                    public_members_url varchar,
                    avatar_url varchar,
                    description varchar,
                    load_ts timestamp(6)
                ) WITH (
                    format = 'PARQUET'
                )""",
        )

    @task()
    def get_max_org_id():
        result = TrinoHook().get_records("SELECT max(id) FROM lakehouse.github.orgs")
        return result[0][0]

    schema = create_github_schema()
    table = create_github_orgs_table()
    max_org_id = get_max_org_id()
    # schema >> table >> max_org_id
    print(max_org_id)

dag = ProcessGithubOrgs()
