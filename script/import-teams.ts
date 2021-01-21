import { writeSync } from "fs";
import { BigQuery } from "@google-cloud/bigquery";
import { gql, GraphQLClient } from "graphql-request";
import { fileSync } from "tmp";

// hatena orgのTeamsの一覧と各teamと関連付けられているrepositoriesをBigQueryに入れるスクリプト
//
// 指定する環境変数
// - GITHUB_TOKEN: repoが付いたgithub token
// - GOOGLE_APPLICATION_CREDENTIALS: Google Cloud APIsを呼び出すための鍵ファイルへのパス
//     - BigQueryデータ編集者とBigQueryジョブユーザーのロールが必要
//     - See Also: https://cloud.google.com/docs/authentication/production?hl=ja
//
// 実行例
// GITHUB_TOKEN=... GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json yarn --silent ts-node script/import-teams.ts

const GITHUB_GRAPHQL_ENDPOINT = "https://api.github.com/graphql";
const GCP_PROJECT_ID = "pull-request-analysis-sample";

(async function main(): Promise<void> {
  const githubToken = process.env.GITHUB_TOKEN;
  const bigquery = new BigQuery({
    projectId: GCP_PROJECT_ID,
  });

  const graphqlClient = new GraphQLClient(GITHUB_GRAPHQL_ENDPOINT, {
    headers: {
      authorization: `Bearer ${githubToken}`,
    },
    timeout: 3600_000,
  });

  const prs = await fetchTeamsWithRepositories(graphqlClient);
  console.log(JSON.stringify(prs, undefined, 2));
  console.log(prs.length);

  // 常に最新を入れていきたいので、一旦テーブルを削除する
  const [tableExists] = await bigquery.dataset("source__github").table("teams").exists();
  if (tableExists) {
    await bigquery.dataset("source__github").table("teams").delete();
  }

  if (prs.length === 0) return;

  // loadに渡せるように一時ファイルに書き込む
  const tmpFile = fileSync();
  writeSync(tmpFile.fd, prs.map(pr => teamNodeToJson(pr)).join("\n"));

  const [job] = await bigquery
    .dataset("source__github")
    .table("teams")
    .load(tmpFile.name, {
      encoding: "UTF-8",
      format: "JSON",
      autodetect: true,
      // APIで新しいフィールドが増えてもスキーマが更新されるようにしておく
      schemaUpdateOptions: ["ALLOW_FIELD_ADDITION"],
    });

  console.log(`Job ${job.id} completed.`);
  const errors = job.status?.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }
})().catch(error => {
  console.error(error);
  process.exit(1);
});

function teamNodeToJson(node: TeamNode): string {
  // repositoriesがBigQueryのnested arrayで入るが
  // view側でflatにするので、このスクリプトではそのままのネストした構造で入れる
  return JSON.stringify({
    name: node.name,
    repositories: node.repositories,
  });
}

type TeamNode = {
  name: string;
  repositories: {
    nodes: {
      nameWithOwner: string;
    }[];
  };
};
async function fetchTeamsWithRepositories(graphqlClient: GraphQLClient): Promise<TeamNode[]> {
  const teams: TeamNode[] = await fetchAllTeamsByQuery(graphqlClient);
  return teams;
}

async function fetchAllTeamsByQuery(graphqlClient: GraphQLClient): Promise<TeamNode[]> {
  const query = gql`
    query($after: String) {
      organization(login: "hatena") {
        teams(first: 10, after: $after) {
          nodes {
            name
            repositories(first: 100) {
              nodes {
                nameWithOwner
              }
            }
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }
  `;

  let after: string | undefined;
  let teams: TeamNode[] = [];
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const data = await graphqlClient.request(query, { after });
    teams = teams.concat(data.organization.teams.nodes);

    if (!data.organization.teams.pageInfo.hasNextPage) break;

    after = data.organization.teams.pageInfo.endCursor;
    console.log(`next: ${after}`);
  }

  return teams;
}

// avoid global scope
export {};
