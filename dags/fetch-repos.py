import datetime
import pendulum
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator

@dag(
    dag_id="process-github-repos",
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(seconds=50),
)
def ProcessGithubRepos():
    create_github_schema = TrinoOperator(
        task_id="create_github_schema",
        sql="""
            CREATE SCHEMA IF NOT EXISTS lakehouse.github WITH (
                location = 's3a://open-source-dashboards/github'
            )""",
    )

    create_github_repos_table = TrinoOperator(
        task_id="create_github_repos_table",
        sql="""
            CREATE TABLE IF NOT EXISTS lakehouse.github.repos (
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
            )""",
    )

    create_github_schema >> create_github_repos_table

dag = ProcessGithubRepos()
