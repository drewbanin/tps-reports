

with commits as (

    select * from {{ ref('gh_commits') }}

),

commit_files as (

    select * from {{ ref('gh_commit_files') }}

),

users as (

    select * from {{ ref('gh_users') }}

),

repos as (

    select * from {{ ref('gh_repos') }}

)

select
    commits.sha as commit_sha,
    repos.name as repo_name,
    commits.author_name as author,
    commits.committer_date as commit_date,
    commit_files.filename,
    commit_files.changes as lines_of_code_changed

from commits
    join users on users.name = commits.committer_name
    join repos on commits.repository_id = repos.id
    join commit_files on commits.sha = commit_files.commit_sha
