WITH teams AS (
  SELECT
    "bookmark" AS teamName
  UNION ALL
  SELECT
    "mackerel"
  UNION ALL
  SELECT
    "syspla"
)
SELECT
  *
FROM
  teams
