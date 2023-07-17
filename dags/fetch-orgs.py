import boto3
import datetime
import pandas
import pendulum
import random
import requests
import string
from typing import Any

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
    def create_lakehouse_github_schema():
        TrinoHook().run("CREATE SCHEMA IF NOT EXISTS lakehouse.github WITH (location = 's3a://open-source-dashboards/lakehouse/github')")
        return "lakehouse.github"

    @task()
    def create_staging_github_schema():
        TrinoHook().run("CREATE SCHEMA IF NOT EXISTS staging.github WITH (location = 's3a://open-source-dashboards/staging/github')")
        return "staging.github"

    @task()
    def create_github_orgs_table(schema: str):
        TrinoHook().run(f"""
            CREATE TABLE IF NOT EXISTS {schema}.orgs (
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
                load_ts timestamp(6),
                repo_update_ts timestamp(6),
                repo_update_repo_id bigint
            ) WITH (
                format = 'PARQUET'
            )""")
        return f"{schema}.orgs"

    @task()
    def get_max_org_id(table: str):
        result = TrinoHook().get_records(f"SELECT coalesce(max(id), 0) FROM {table}")
        return result[0][0]

    @task
    def fetch_new_orgs(max_org_id: int):
        def finalize_df(df):
            df['load_ts'] = datetime.datetime.today()
            df = df.astype({"description": str})
            return df

        df = None
        for _ in range(400):
            df = pandas.concat([df, pandas.read_json(f"https://api.github.com/organizations?per_page=100&since={max_org_id}", storage_options=GITHUB_HTTP_HEADERS)])
            max_org_id = max(max_org_id, df["id"].max())

        return finalize_df(df)

    @task
    def write_orgs_to_s3(df: pandas.DataFrame):
        staging_table_name =''.join(random.choice(string.ascii_lowercase) for i in range(32))
        df.to_parquet(
            f"s3://{S3_BUCKET}/staging/github/{staging_table_name}/orgs.parquet",
            storage_options={
                "key": S3_ACCESS_KEY_ID,
                "secret": S3_SECRET_ACCESS_KEY,
                "client_kwargs": {'endpoint_url': S3_ENDPOINT}
            }
        )
        return staging_table_name

    @task()
    def create_staging_table(schema: str, staging_table_name: str):
        TrinoHook().run(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{staging_table_name} (
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
                load_ts timestamp
            ) WITH (
                format = 'PARQUET',
                external_location = 's3a://{S3_BUCKET}/staging/github/{staging_table_name}/'
            )""")
        return f"{schema}.{staging_table_name}"

    @task()
    def merge_staging_table_into_lakehouse(staging_table: str, lakehouse_table: str):
        TrinoHook().run(f"""
            MERGE INTO {lakehouse_table} AS t
            USING (SELECT * FROM {staging_table}) AS u
            ON t.id = u.id
            WHEN NOT MATCHED THEN INSERT VALUES (
                u.id,
                u.node_id,
                u.login,
                u.url,
                u.repos_url,
                u.events_url,
                u.hooks_url,
                u.issues_url,
                u.members_url,
                u.public_members_url,
                u.avatar_url,
                u.description,
                cast(u.load_ts as timestamp(6)),
                NULL,
                NULL)""")
        return staging_table

    @task()
    def drop_staging_table(staging_table: str):
        TrinoHook().run(f"""
            DROP TABLE {staging_table}""")

    @task()
    def delete_s3_files(staging_table: str, staging_table_name: str):
        s3 = boto3.resource('s3', aws_access_key_id=S3_ACCESS_KEY_ID, aws_secret_access_key=S3_SECRET_ACCESS_KEY, endpoint_url=S3_ENDPOINT)
        bucket = s3.Bucket(S3_BUCKET)
        bucket.objects.filter(Prefix=f"staging/github/{staging_table_name}/").delete()

    lakehouse_schema = create_lakehouse_github_schema()
    staging_schema = create_staging_github_schema()

    lakehouse_table = create_github_orgs_table(lakehouse_schema)
    max_org_id = get_max_org_id(lakehouse_table)
    new_orgs = fetch_new_orgs(max_org_id)
    staging_table_name = write_orgs_to_s3(new_orgs)
    staging_table = create_staging_table(staging_schema, staging_table_name)
    staging_table = merge_staging_table_into_lakehouse(staging_table, lakehouse_table)
    drop_staging_table(staging_table)
    delete_s3_files(staging_table, staging_table_name)

dag = ProcessGithubOrgs()
