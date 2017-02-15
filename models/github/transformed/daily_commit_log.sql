{{
    config(
        materialized='table'
    )
}}

with commit_summary as (

    select * from {{ ref('commit_summary') }}

),

dates as (

    select * from {{ ref('dates') }} where date < now()

),

daily_commits as (

  select
      repo_name,
      date_trunc('day', commit_date) as date,
      count_commits,
      count_changed_files,
      count_loc_changed

  from commit_summary

),

daily_commits_with_next as (

    select *,
        lead(date) over (partition by repo_name order by date) as next_date

    from daily_commits

),

daily_commits_filled as (

  select
      repo_name,
      d.date,
      d.nice_date,
      case
        when c.date != d.date then 0
        else c.count_commits
      end as count_commits,
      case
        when c.date != d.date then 0
        else c.count_changed_files
      end as count_changed_files,
      case
        when c.date != d.date then 0
        else c.count_loc_changed
      end as count_loc_changed

  from daily_commits_with_next as c
  join dates d
    on d.date >= c.date and (c.next_date is null or d.date < c.next_date)

)

select * from daily_commits_filled
