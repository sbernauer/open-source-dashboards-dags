import datetime
import pandas
import pendulum
import random
import requests
import string

from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.models import Variable

S3_BUCKET = "open-source-dashboards"
S3_ACCESS_KEY_ID = Variable.get("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = Variable.get("S3_SECRET_ACCESS_KEY")
S3_ENDPOINT = "https://s3-eu-central-2.ionoscloud.com"
GITHUB_HTTP_HEADERS = {"Authorization": f"Bearer {Variable.get('GITHUB_API_TOKEN')}"}

@dag(
    dag_id="process-github-orgs",
    schedule_interval="*/5 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
)
def ProcessGithubOrgs():
    @task()
    def create_github_schema():
        TrinoHook().run("CREATE SCHEMA IF NOT EXISTS lakehouse.github WITH (location = 's3a://open-source-dashboards/github')")
        return True

    @task()
    def create_github_orgs_table(dummy: bool):
        TrinoHook().run("""
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
            )""")
        return True

    @task()
    def get_max_org_id(dummy: bool):
        result = TrinoHook().get_records("SELECT coalesce(max(id), 0) FROM lakehouse.github.orgs")
        return result[0][0]

    @task
    def fetch_new_orgs(max_org_id: int):
        df = None
        for _ in range(250):
            df = pandas.concat([df, pandas.read_json(f"https://api.github.com/organizations?per_page=100&since={max_org_id}", storage_options=GITHUB_HTTP_HEADERS)])
            max_org_id = max(max_org_id, df["id"].max())

        return df

    @task()
    def write_orgs_to_s3(df: pandas.DataFrame, starting_org_id: int):
        s3_folder_name =''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(20))
        s3_folder = f"s3://{S3_BUCKET}/staging/default/{s3_folder_name}"
        df.to_parquet(
            f"{s3_folder}/orgs-starting-from-{starting_org_id}.parquet",
            storage_options={
                "key": S3_ACCESS_KEY_ID,
                "secret": S3_SECRET_ACCESS_KEY,
                "client_kwargs": {'endpoint_url': S3_ENDPOINT}
            }
        )
        return (s3_folder_name, s3_folder)

    schema = create_github_schema()
    table = create_github_orgs_table(schema)
    max_org_id = get_max_org_id(table)
    df = fetch_new_orgs(max_org_id)
    (s3_folder_name, s3_folder) = write_orgs_to_s3(df, max_org_id)

dag = ProcessGithubOrgs()
