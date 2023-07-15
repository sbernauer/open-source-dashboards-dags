import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator

@dag(
    dag_id="process-github-orgs",
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(seconds=50),
)
def ProcessGithubOrgs():
    create_github_schema = TrinoOperator(
        task_id="create_github_schema",
        sql="""
            CREATE SCHEMA IF NOT EXISTS lakehouse.github WITH (
                location = 's3a://open-source-dashboards/github'
            )""",
    )

    create_github_orgs_table = TrinoOperator(
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

    create_github_schema >> create_github_orgs_table

dag = ProcessGithubOrgs()
