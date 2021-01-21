WITH teams AS (
  SELECT
    "bookmark" AS teamName
  UNION ALL
  SELECT
    "mackerel"
  UNION ALL
  SELECT
    "blog"
)
SELECT
  *
FROM
  teams
