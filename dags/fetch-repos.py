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
# Using a different token to avoid rate-limit
GITHUB_HTTP_HEADERS = {"Authorization": f"Bearer {Variable.get('GITHUB_API_TOKEN_2')}"}

@dag(
    dag_id="process-github-repos",
    schedule_interval="*/10 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
)
def ProcessGithubRepos():
    @task()
    def create_lakehouse_github_schema():
        TrinoHook().run("CREATE SCHEMA IF NOT EXISTS lakehouse.github WITH (location = 's3a://open-source-dashboards/lakehouse/github')")
        return "lakehouse.github"

    @task()
    def create_staging_github_schema():
        TrinoHook().run("CREATE SCHEMA IF NOT EXISTS staging.github WITH (location = 's3a://open-source-dashboards/staging/github')")
        return "staging.github"

    @task()
    def create_github_repos_table(schema: str):
        TrinoHook().run(f"""
            CREATE TABLE IF NOT EXISTS {schema}.repos (
                id bigint,
                node_id varchar,
                name varchar,
                full_name varchar,
                private boolean,
                owner row(
                    login varchar,
                    id bigint,
                    node_id varchar,
                    avatar_url varchar,
                    gravatar_id varchar,
                    url varchar,
                    html_url varchar,
                    followers_url varchar,
                    following_url varchar,
                    gists_url varchar,
                    starred_url varchar,
                    subscriptions_url varchar,
                    organizations_url varchar,
                    repos_url varchar,
                    events_url varchar,
                    received_events_url varchar,
                    type varchar,
                    site_admin boolean
                ),
                html_url varchar,
                description varchar,
                fork boolean,
                url varchar,
                forks_url varchar,
                keys_url varchar,
                collaborators_url varchar,
                teams_url varchar,
                hooks_url varchar,
                issue_events_url varchar,
                events_url varchar,
                assignees_url varchar,
                branches_url varchar,
                tags_url varchar,
                blobs_url varchar,
                git_tags_url varchar,
                git_refs_url varchar,
                trees_url varchar,
                statuses_url varchar,
                languages_url varchar,
                stargazers_url varchar,
                contributors_url varchar,
                subscribers_url varchar,
                subscription_url varchar,
                commits_url varchar,
                git_commits_url varchar,
                comments_url varchar,
                issue_comment_url varchar,
                contents_url varchar,
                compare_url varchar,
                merges_url varchar,
                archive_url varchar,
                downloads_url varchar,
                issues_url varchar,
                pulls_url varchar,
                milestones_url varchar,
                notifications_url varchar,
                labels_url varchar,
                releases_url varchar,
                deployments_url varchar,
                created_at timestamp(6),
                updated_at timestamp(6),
                pushed_at timestamp(6),
                git_url varchar,
                ssh_url varchar,
                clone_url varchar,
                svn_url varchar,
                homepage varchar,
                size bigint,
                stargazers_count bigint,
                watchers_count bigint,
                language varchar,
                has_issues boolean,
                has_projects boolean,
                has_downloads boolean,
                has_wiki boolean,
                has_pages boolean,
                has_discussions boolean,
                forks_count bigint,
                mirror_url varchar,
                archived boolean,
                disabled varchar,
                open_issues_count bigint,
                license row(key varchar, name varchar, spdx_id varchar, url varchar, node_id varchar),
                allow_forking boolean,
                is_template boolean,
                web_commit_signoff_required boolean,
                topics array(varchar),
                visibility varchar,
                forks bigint,
                open_issues bigint,
                watchers bigint,
                default_branch varchar,
                permissions row(admin boolean, maintain boolean, push boolean,triage boolean, pull boolean),
                load_ts timestamp(6)
            ) WITH (
                format = 'PARQUET'
            )""")
        return f"{schema}.repos"

    @task()
    def get_orgs_that_need_repos_update():
        # TODO: Ensure lakehouse.github.orgs exists by e.g. calling the needed create schema and create table
        result = TrinoHook().get_records(f"""
            SELECT id
            FROM lakehouse.github.orgs
            WHERE repo_update_ts IS NULL
            ORDER BY id
            LIMIT 1000""")
        return [item[0] for item in result]

    @task
    def fetch_repos_for_orgs(orgs_that_need_repo_update: list[int]):
        orgs_updated = []

        requests_left = 80 # We run every 10 minutes and have 5000 req/hour => 833 req/10 min
        df = None
        for org_id in orgs_that_need_repo_update:
            requests_left -= 1
            if requests_left == 0:
                if df is None:
                    raise Exception(f"df is None. This should not happen")
                return df
            response = requests.get(f"https://api.github.com/orgs/{org_id}/repos?per_page=100")
            response.raise_for_status()

            df_for_org = pandas.DataFrame.from_dict(response.json())
            while "next" in response.links and "url" in response.links["next"]:
                next_url = response.links["next"]["url"]
                requests_left -= 1
                if requests_left == 0:
                    if df is None:
                        raise Exception(f"df was null. Maybe org with id {org_id} has too many repos?")
                    return df
                response = requests.get(next_url)
                df_for_org = pandas.concat([df_for_org, pandas.DataFrame.from_dict(response.json())])

            df = pandas.concat([df, df_for_org])
            if org_id not in orgs_updated:
                orgs_updated += [org_id]

        df['load_ts']= datetime.datetime.today()
        return (df, orgs_updated)

    @task
    def write_repos_to_s3(df: pandas.DataFrame):
        staging_table_name =''.join(random.choice(string.ascii_lowercase) for i in range(32))
        df.to_parquet(
            f"s3://{S3_BUCKET}/staging/github/{staging_table_name}/repos.parquet",
            storage_options={
                "key": S3_ACCESS_KEY_ID,
                "secret": S3_SECRET_ACCESS_KEY,
                "client_kwargs": {'endpoint_url': S3_ENDPOINT}
            }
        )
        return staging_table_name

    lakehouse_schema = create_lakehouse_github_schema()
    staging_schema = create_staging_github_schema()

    lakehouse_table = create_github_repos_table(lakehouse_schema)
    orgs_that_need_repos_update = get_orgs_that_need_repos_update()
    repos, orgs_updated = fetch_repos_for_orgs(orgs_that_need_repos_update)
    staging_table_name = write_orgs_to_s3(repos)

dag = ProcessGithubRepos()
