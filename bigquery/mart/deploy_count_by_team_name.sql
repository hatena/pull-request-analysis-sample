WITH all_team_pull_requests AS (
  SELECT
  teamName,
  repositoryNameWithOwner,
  TIMESTAMP_DIFF(mergedAt, firstCommittedAt, hour) AS process_time_hour,
  pull_requests.*,
FROM
  `pull-request-analysis-sample`.source__github.pull_requests
  INNER JOIN `pull-request-analysis-sample`.warehouse.teams_repositories_relation AS teams_repositories_relation ON teams_repositories_relation.repositoryNameWithOwner = pull_requests.repository.nameWithOwner
WHERE
  baseRefName IN ('master', 'main')
  AND (
    title LIKE 'Release %'
    OR title LIKE '【リリース】%'
    OR (('Release') IN unnest(labelNames))
    OR (('release') IN unnest(labelNames))
  )
),
-- 対象範囲を出す
start_and_end_days AS (
  SELECT
    MIN(DATE_TRUNC(DATE(mergedAt), DAY)) AS start_day,
    CURRENT_DATE() AS end_day
  FROM
    all_team_pull_requests
),
-- 日付の集合を作る
date_seq AS (
  SELECT
    date
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY(
        (
          SELECT
            start_day
          FROM
            start_and_end_days
        ),
        (
          SELECT
            end_day
          FROM
            start_and_end_days
        )
      )
    ) AS date
  ORDER BY
    date
),
github_pr_logs AS (
  SELECT
    DATE_TRUNC(DATE(mergedAt), DAY) AS merged_at_trunc,
    all_team_pull_requests.*
  FROM
    all_team_pull_requests
),
-- 1日ごとに各指標を集計していく
github_pr_logs_with_median AS (
  SELECT
    date_seq.date,
    PERCENTILE_CONT(github_pr_logs.process_time_hour, 0.5) OVER (PARTITION BY date_seq.date, teamName) AS process_time_median,
    PERCENTILE_CONT(github_pr_logs.process_time_hour, 0.5) OVER (PARTITION BY date_seq.date, teamName) AS time_to_merge_median,
    COUNT(*) OVER (PARTITION BY date_seq.date, teamName) AS deploy_count,
    github_pr_logs.*
  FROM
    date_seq
    INNER JOIN github_pr_logs ON date_seq.date >= github_pr_logs.merged_at_trunc
    AND DATE(github_pr_logs.merged_at_trunc) >= DATE_SUB(date_seq.date, INTERVAL 28 DAY)
)
SELECT
  date,
  teamName,
  MAX(deploy_count) AS deploy_count,
  MAX(process_time_median) AS process_time_hours_median,
  MAX(time_to_merge_median) AS time_to_merge_hours_median,
FROM
  github_pr_logs_with_median
GROUP BY
  date,
  teamName
ORDER BY
  date DESC
