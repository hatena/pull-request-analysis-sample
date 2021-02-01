SELECT
  teamName,
  TIMESTAMP_DIFF(mergedAt, firstCommittedAt, hour) AS process_time_hour,
  pull_requests.*,
FROM
  `pull-request-analysis-sample`.source__github.pull_requests
  INNER JOIN `pull-request-analysis-sample.warehouse.teams_repositories_relation` AS teams_repositories_relation ON teams_repositories_relation.repositoryNameWithOwner = pull_requests.repository.nameWithOwner
WHERE
  author.typename != "Bot"
