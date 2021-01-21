SELECT
  name as teamName,
  repos.nameWithOwner as repositoryNameWithOwner
FROM
  `pull-request-analysis-sample.source__github.teams` teams,
  UNNEST(repositories.nodes) AS repos
  -- teams_to_show に入っているチームだけに絞り込む
  INNER JOIN `pull-request-analysis-sample.warehouse.teams_to_show` ON teams.name = `pull-request-analysis-sample.warehouse.teams_to_show`.teamName
