import { writeSync } from "fs";
import { BigQuery } from "@google-cloud/bigquery";
import retry from "async-retry";
import { add, addDays, max, min, parseISO, startOfToday } from "date-fns";
import { gql, GraphQLClient } from "graphql-request";
import { fileSync } from "tmp";

// hatena orgのPullRequestの統計情報をBigQueryに入れるスクリプト
//
// 指定する環境変数
// - GITHUB_TOKEN: repoが付いたgithub token
// - GOOGLE_APPLICATION_CREDENTIALS: Google Cloud APIsを呼び出すための鍵ファイルへのパス
//     - BigQueryデータ編集者とBigQueryジョブユーザーのロールが必要
//     - See Also: https://cloud.google.com/docs/authentication/production?hl=ja
// - START_DATE(optional): 集計対象の始点。ISO8601形式。デフォルトは前日0:00
// - END_DATE(optional): 集計対象の終点。ISO8601形式。デフォルトは当日0:00
// - USE_REINDEX_TABLE(optional): 1を入れておくとpull_requests_reindexというテーブルにデータを入れる。次のようにすることで影響を最小限にデータを入れ直せる
//   - コンソールなどでreindexのテーブルを消しておく
//   - スクリプトを使ってreindex用のテーブルにデータを入れる
//   - reindex用のテーブルの中身を確認
//   - BigQueryのコンソールなどから本データのテーブルへデータをコピー
//
// 実行例
// GITHUB_TOKEN=... GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json START_DATE=2020-10-05 END_DATE=2020-10-12 yarn --silent ts-node script/import-pull-request.ts

// どのgithub organizationsのデータを取得するか
const GITHUB_ORG_NAME = "hatena";
const GITHUB_GRAPHQL_ENDPOINT = "https://api.github.com/graphql";
// BigQueryのプロジェクトID
const GCP_PROJECT_ID = "pull-request-analysis-sample";
// GitHubのAPIからどの程度並列にデータを取得するか
const CONCURRENT_FETCH_COUNT = 10;

(async function main(): Promise<void> {
  const startDateISO = process.env.START_DATE;
  const endDateISO = process.env.END_DATE;
  const githubToken = process.env.GITHUB_TOKEN;
  const useReindexTable = process.env.USE_REINDEX_TABLE === "1";

  const endDate = endDateISO ? parseISO(endDateISO) : startOfToday();
  const startDate = startDateISO ? parseISO(startDateISO) : addDays(endDate, -1);

  const bqTableName = useReindexTable ? "pull_requests_reindex" : "pull_requests";

  console.log("input parameters: ", {
    startDate,
    endDate,
    bqTableName,
  });

  const bigquery = new BigQuery({
    projectId: GCP_PROJECT_ID,
  });

  const graphqlClient = new GraphQLClient(GITHUB_GRAPHQL_ENDPOINT, {
    headers: {
      authorization: `Bearer ${githubToken}`,
    },
    timeout: 3600_000,
  });

  // 重複を防ぐため、先に該当範囲をDELETEしておく
  const [tableExists] = await bigquery.dataset("source__github").table(bqTableName).exists();
  if (tableExists) {
    await bigquery.query({
      query: `DELETE FROM \`source__github.${bqTableName}\`
                WHERE @startDate <= mergedAt
                  AND mergedAt <= @endDate`,
      params: {
        startDate: startDate.toISOString(),
        endDate: endDate.toISOString(),
      },
    });
  }

  // importする期間が長くてもある程度の期間でimportできるように
  // 1週間単位のレンジのデータを作る。ここを短くしすぎると
  // BigQueryのrate limitに当たるので注意。
  // 新しいものから入れていきたいので日時降順にしている。
  const fromAndTos: Array<{ from: Date; to: Date }> = [];
  for (let to = endDate; to > startDate; to = add(to, { days: -7 })) {
    const from = max([add(to, { days: -7 }), startDate]);
    fromAndTos.push({ from, to });
  }

  for (const { from, to } of fromAndTos) {
    await importPullRequestsByDateRange({
      bqClient: bigquery,
      graphqlClient,
      bqTableName,
      from,
      to,
    });
  }
})().catch(error => {
  console.error(error);
  process.exit(1);
});

async function importPullRequestsByDateRange(
  params: Readonly<{
    bqClient: BigQuery;
    graphqlClient: GraphQLClient;
    bqTableName: "pull_requests" | "pull_requests_reindex";
    from: Date;
    to: Date;
  }>
): Promise<void> {
  console.log(`importing... ${params.from.toISOString()}〜${params.to.toISOString()}`);

  const prs = await fetchMergedPullRequests(params.graphqlClient, params.from, params.to);
  if (prs.length === 0) return;

  // loadに渡せるように一時ファイルに書き込む
  const tmpFile = fileSync();
  writeSync(tmpFile.fd, prs.map(pr => pullRequestNodeToJson(pr)).join("\n"));

  const [job] = await params.bqClient
    .dataset("source__github")
    .table(params.bqTableName)
    .load(tmpFile.name, {
      encoding: "UTF-8",
      format: "JSON",
      autodetect: true,
      // APIで新しいフィールドが増えてもスキーマが更新されるようにしておく
      schemaUpdateOptions: ["ALLOW_FIELD_ADDITION"],
    });

  const errors = job.status?.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }
}

function pullRequestNodeToJson(node: PullRequestNode): string {
  return JSON.stringify({
    id: node.id,
    title: node.title,
    author: {
      login: node.author.login,
      typename: node.author.__typename,
    },
    url: node.url,
    createdAt: node.createdAt,
    mergedAt: node.mergedAt,
    additions: node.additions,
    deletions: node.deletions,
    firstCommittedAt: node.commits.nodes[0].commit.authoredDate,
    number: node.number,
    repository: { nameWithOwner: node.repository.nameWithOwner },
    baseRefName: node.baseRefName,
    headRefName: node.headRefName,
    reviews: { totalCount: node.reviews.totalCount },
    labelNames: node.labels.nodes.map(l => l.name),
  });
}

type PullRequestNode = {
  id: string;
  title: string;
  author: {
    __typename: string;
    login: string;
  };
  url: string;
  number: number;
  repository: {
    nameWithOwner: string;
  };
  createdAt: string;
  mergedAt: string;
  additions: number;
  deletions: number;
  commits: {
    nodes: {
      commit: {
        authoredDate: string;
      };
    }[];
  };
  baseRefName: string;
  headRefName: string;
  reviews: {
    totalCount: number;
  };
  labels: {
    nodes: {
      name: string;
    }[];
  };
};
async function fetchMergedPullRequests(
  graphqlClient: GraphQLClient,
  mergedFrom: Date,
  mergedTo: Date
): Promise<PullRequestNode[]> {
  // mergedAtの範囲が広すぎるとtimeoutすることがあるので、一定期間に区切って探す
  let prs: PullRequestNode[] = [];

  const fromAndTos: Array<{ from: Date; to: Date }> = [];
  for (let from = mergedFrom; from < mergedTo; from = add(from, { hours: 1 })) {
    const to = min([add(from, { hours: 1 }), mergedTo]);
    fromAndTos.push({ from, to });
  }

  let roundCount = 0;
  while (fromAndTos.length != 0) {
    // CONCURRENT_FETCH_COUNT 件ずつ Promise.all でいっぺんに concurrent に取得する (時短テク)
    const proc = fromAndTos.splice(0, CONCURRENT_FETCH_COUNT);
    const gotPrs = await Promise.all([
      ...proc.map(({ from, to }) => fetchAllPullRequestsByDateRange(from, to, graphqlClient)),
    ]);
    prs = prs.concat(...gotPrs.flat(2));

    roundCount++;
    if (roundCount % 5 === 0) {
      logRateLimit(graphqlClient);
    }

    // GitHubのrate limitを超えないように制御する
    // 1分間で最大でも CONCURRENT_FETCH_COUNT * 10 回しかAPIを叩かないように
    await sleep(6000);
  }

  return prs;
}

async function logRateLimit(graphqlClient: GraphQLClient): Promise<void> {
  const query = gql`
    query {
      viewer {
        login
      }
      rateLimit {
        limit
        cost
        remaining
        resetAt
      }
    }
  `;

  const data = await graphqlClient.request(query);
  console.log(
    `rateLimit: remaining ${data.rateLimit.remaining} of ${data.rateLimit.limit} (resetAt: ${data.rateLimit.resetAt})`
  );
}

async function fetchAllPullRequestsByDateRange(
  from: Date,
  to: Date,
  graphqlClient: GraphQLClient
): Promise<PullRequestNode[]> {
  console.log(`fetching... ${from.toISOString()}〜${to.toISOString()}`);

  const query = `org:${GITHUB_ORG_NAME} is:pr is:merged merged:${from.toISOString()}..${to.toISOString()}`;
  return fetchAllPullRequestsByQuery(graphqlClient, query);
}

async function fetchAllPullRequestsByQuery(
  graphqlClient: GraphQLClient,
  searchQuery: string
): Promise<PullRequestNode[]> {
  const query = gql`
    query($after: String) {
      search(type: ISSUE, first: 100, query: "${searchQuery}", after: $after) {
        nodes {
          ... on PullRequest {
            id
            title
            author {
              __typename
              login
            }
            url
            number
            repository {
              nameWithOwner
            }
            createdAt
            mergedAt
            additions
            deletions
            # for lead time
            commits(first:1) {
              nodes {
                commit {
                  authoredDate
                }
              }
            }
            baseRefName
            headRefName
            reviews {
              totalCount
            }
            # ラベルが100件超えると取れないが諦める
            labels(first: 100) {
              nodes {
                name
              }
            }
          }
        }
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }
  `;

  let after: string | undefined;
  let prs: PullRequestNode[] = [];
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const data = await retry(
      async () => {
        return await graphqlClient.request(query, { after });
      },
      {
        retries: 3,
      }
    );
    prs = prs.concat(data.search.nodes);

    if (!data.search.pageInfo.hasNextPage) break;

    after = data.search.pageInfo.endCursor;
    console.log(`next: ${after}`);
  }

  return prs;
}

function sleep(msec: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, msec));
}

// avoid global scope
export {};
