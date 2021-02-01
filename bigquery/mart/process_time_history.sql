WITH pull_requests AS (
  SELECT
    TIMESTAMP_DIFF(mergedAt, firstCommittedAt, SECOND) AS process_time,
    pull_requests.*,
  FROM
    `pull-request-analysis-sample`.source__github.pull_requests
  WHERE
    -- botを除く
    author.typename != "Bot"
),
-- 対象範囲を出す
start_and_end_days AS (
  SELECT
    MIN(DATE_TRUNC(DATE(mergedAt), DAY)) AS start_day,
    MAX(DATE_TRUNC(DATE(mergedAt), DAY)) AS end_day
  FROM
    pull_requests
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
    pull_requests.*
  FROM
    pull_requests
),
-- 1日ごとに各指標を集計していく
-- 28日間移動平均
github_pr_logs_with_median AS (
  SELECT
    date_seq.date,
    PERCENTILE_CONT(github_pr_logs.process_time, 0.5) OVER (PARTITION BY date_seq.date) AS process_time_median,
    COUNT(*) OVER (PARTITION BY date_seq.date) AS pr_count,
    github_pr_logs.*
  FROM
    date_seq
  INNER JOIN
    github_pr_logs
  ON
    date_seq.date >= github_pr_logs.merged_at_trunc
    AND DATE(github_pr_logs.merged_at_trunc) >= DATE_SUB(date_seq.date, INTERVAL 28 DAY)
)
SELECT
  github_pr_logs_with_median.date,
  MAX(github_pr_logs_with_median.pr_count) AS pr_count,
  MAX(
    github_pr_logs_with_median.process_time_median
  ) / 3600 as process_time_hour_median,
FROM
  github_pr_logs_with_median
GROUP BY
  date
ORDER BY
  date DESC
