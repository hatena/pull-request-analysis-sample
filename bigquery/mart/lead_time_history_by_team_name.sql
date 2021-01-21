WITH all_team_pull_requests AS (
  SELECT
    teamName,
    TIMESTAMP_DIFF(mergedAt, firstCommittedAt, hour) AS lead_time_hour,
    pull_requests.*,
  FROM
    `pull-request-analysis-sample`.source__github.pull_requests
    INNER JOIN `pull-request-analysis-sample.warehouse.teams_repositories_relation` AS teams_repositories_relation ON teams_repositories_relation.repositoryNameWithOwner = pull_requests.repository.nameWithOwner
  WHERE
    author.typename != "Bot"
),
-- 対象範囲を出す
start_and_end_days AS (
  SELECT
    MIN(DATE_TRUNC(DATE(mergedAt), DAY)) AS start_day,
    MAX(DATE_TRUNC(DATE(mergedAt), DAY)) AS end_day
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
-- 28日間移動平均
github_pr_logs_with_median AS (
  SELECT
    concat(teamName, date_seq.date) as join_key,
    date_seq.date,
    PERCENTILE_CONT(github_pr_logs.lead_time_hour, 0.25) OVER (PARTITION BY date_seq.date, teamName) AS time_to_merge_25pctile,
    PERCENTILE_CONT(github_pr_logs.lead_time_hour, 0.5) OVER (PARTITION BY date_seq.date, teamName) AS time_to_merge_median,
    PERCENTILE_CONT(github_pr_logs.lead_time_hour, 0.75) OVER (PARTITION BY date_seq.date, teamName) AS time_to_merge_75pctile,
    COUNT(*) OVER (PARTITION BY date_seq.date, teamName) AS pr_count,
    github_pr_logs.*
  FROM
    date_seq
    INNER JOIN github_pr_logs ON date_seq.date >= github_pr_logs.merged_at_trunc
    AND DATE(github_pr_logs.merged_at_trunc) >= DATE_SUB(date_seq.date, INTERVAL 28 DAY)
),
-- 7日間移動平均
github_pr_logs_with_7d_median AS (
  SELECT
    concat(teamName, date_seq.date) as join_key,
    date_seq.date,
    PERCENTILE_CONT(github_pr_logs.lead_time_hour, 0.25) OVER (PARTITION BY date_seq.date, teamName) AS time_to_merge_25pctile,
    PERCENTILE_CONT(github_pr_logs.lead_time_hour, 0.5) OVER (PARTITION BY date_seq.date, teamName) AS time_to_merge_median,
    PERCENTILE_CONT(github_pr_logs.lead_time_hour, 0.75) OVER (PARTITION BY date_seq.date, teamName) AS time_to_merge_75pctile,
    COUNT(*) OVER (PARTITION BY date_seq.date, teamName) AS pr_count,
    github_pr_logs.*
  FROM
    date_seq
    INNER JOIN github_pr_logs ON date_seq.date >= github_pr_logs.merged_at_trunc
    AND DATE(github_pr_logs.merged_at_trunc) >= DATE_SUB(date_seq.date, INTERVAL 7 DAY)
)
SELECT
  github_pr_logs_with_median.date,
  github_pr_logs_with_median.teamName,

  -- 28d
  MAX(github_pr_logs_with_median.pr_count) AS pr_count,
  MAX(
    github_pr_logs_with_median.time_to_merge_median
  ) as time_to_merge_hours_median,
  MAX(
    github_pr_logs_with_median.time_to_merge_25pctile
  ) as time_to_merge_hours_25pctile,
  MAX(
    github_pr_logs_with_median.time_to_merge_75pctile
  ) as time_to_merge_hours_75pctile,

  -- 7d
  MAX(github_pr_logs_with_7d_median.pr_count) AS pr_count_7d,
  MAX(
    github_pr_logs_with_7d_median.time_to_merge_median
  ) AS time_to_merge_hours_7d_median,
  MAX(
    github_pr_logs_with_7d_median.time_to_merge_25pctile
  ) AS time_to_merge_hours_7d_25pctile,
  MAX(
    github_pr_logs_with_7d_median.time_to_merge_75pctile
  ) AS time_to_merge_hours_7d_75pctile,
FROM
  github_pr_logs_with_median
  INNER JOIN github_pr_logs_with_7d_median USING (join_key)
GROUP BY
  date,
  teamName
ORDER BY
  date DESC
