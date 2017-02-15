
with commit_log as (

    select * from {{ ref('commit_log') }}

),

commit_summary as (

  select
    commit_sha,
    repo_name,
    commit_date,
    author,

    count(distinct commit_sha) as count_commits,
    count(distinct filename) as count_changed_files,
    sum(lines_of_code_changed) as count_loc_changed

  from commit_log
  group by 1, 2, 3, 4

)

select * from commit_summary
