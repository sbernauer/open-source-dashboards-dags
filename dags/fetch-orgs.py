import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.models import Variable

@dag(
    dag_id="process-github-orgs",
    schedule_interval="*/5 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=4),
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
        sql = """
        MERGE INTO lakehouse.github.orgs AS t
        USING (
            SELECT * FROM (VALUES"""

        for _ in range(200):
            github_api_token = Variable.get("GITHUB_API_TOKEN")
            headers = {"Authorization": f"Bearer {github_api_token}"}
            response = requests.request("GET", f"https://api.github.com/organizations?per_page=100&since={max_org_id}", headers=headers)
            response.raise_for_status()
            jsonResponse = response.json()

            for org in jsonResponse:
                max_org_id = max(max_org_id, org["id"])

                id = org["id"]
                node_id = "NULL" if org["node_id"] is None else "'" + org["node_id"].replace("'", "''") + "'"
                login = "NULL" if org["login"] is None else "'" + org["login"].replace("'", "''") + "'"
                url = "NULL" if org["url"] is None else "'" + org["url"].replace("'", "''") + "'"
                repos_url = "NULL" if org["repos_url"] is None else "'" + org["repos_url"].replace("'", "''") + "'"
                events_url = "NULL" if org["events_url"] is None else "'" + org["events_url"].replace("'", "''") + "'"
                hooks_url = "NULL" if org["hooks_url"] is None else "'" + org["hooks_url"].replace("'", "''") + "'"
                issues_url = "NULL" if org["issues_url"] is None else "'" + org["issues_url"].replace("'", "''") + "'"
                members_url = "NULL" if org["members_url"] is None else "'" + org["members_url"].replace("'", "''") + "'"
                public_members_url = "NULL" if org["public_members_url"] is None else "'" + org["public_members_url"].replace("'", "''") + "'"
                avatar_url = "NULL" if org["avatar_url"] is None else "'" + org["avatar_url"].replace("'", "''") + "'"
                description = "NULL" if org["description"] is None else "'" + org["description"].replace("'", "''") + "'"

                sql += f"""
                        ({id}, {node_id}, {login}, {url}, {repos_url}, {events_url}, {hooks_url}, {issues_url}, {members_url}, {public_members_url}, {avatar_url}, {description}),"""

        print(sql)
        sql = sql.removesuffix(",")
        sql += """) AS u(id, node_id, login, url, repos_url, events_url, hooks_url, issues_url, members_url, public_members_url, avatar_url, description)) AS u
        ON u.id = t.id
        WHEN NOT MATCHED THEN INSERT VALUES (u.id, u.node_id, u.login, u.url, u.repos_url, u.events_url, u.hooks_url, u.issues_url, u.members_url, u.public_members_url, u.avatar_url, u.description, now())"""

        return sql

    @task()
    def merge_new_org(sql: str):
        TrinoHook().run(sql)
        return True

    schema = create_github_schema()
    table = create_github_orgs_table(schema)
    max_org_id = get_max_org_id(table)
    new_orgs = fetch_new_orgs(max_org_id)
    merge_new_org(new_orgs)

dag = ProcessGithubOrgs()
