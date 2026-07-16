# Apache Spark

## Pre-flight Checks

Before the first code edit or running test in a session, ensure a clean working environment. DO NOT skip these checks:

1. Run `git remote -v` to identify the personal fork and upstream (`apache/spark`). If unclear, ask the user to configure their remotes following the standard convention (`origin` for the fork, `upstream` for `apache/spark`).
2. If the latest commit on `<upstream>/master` is more than a day old (check with `git log -1 --format="%ci" <upstream>/master`), run `git fetch <upstream> master`.
3. If there are uncommitted changes (check with `git status`), ask the user to stash them before proceeding.
4. Switch to the appropriate branch:
   - **Existing PR**: resolve the PR branch name via `gh api repos/apache/spark/pulls/<number> --jq '.head.ref'`, then look for a local branch matching that name. If found, switch to it and inform the user. If not found, ask whether to fetch it or if there is a local branch under a different name.
   - **New edits**: ask the user to choose: create a new git worktree from `<upstream>/master` and work from there (recommended), or create and switch to a new branch from `<upstream>/master`.
   - **Running tests**: use `<upstream>/master`.

## Development Notes

SQL golden file tests are managed by `SQLQueryTestSuite` and its variants. Read the class documentation before running or updating these tests. DO NOT edit the generated golden files (`.sql.out`) directly. Always regenerate them when needed, and carefully review the diff to make sure it's expected.

Spark Connect protocol is defined in proto files under `sql/connect/common/src/main/protobuf/`. Read the README there before modifying proto definitions.

Avoid introducing non-ASCII characters in code or comments. String literals may contain non-ASCII when the content requires it (error messages, test data, etc.). Identifiers are ASCII by convention. The common failure mode is typographic characters (em-dash, smart quotes, ellipsis, non-breaking space) sneaking into comments; scalastyle flags some of these. Spot-check before committing: `grep -rn -P "[^\x00-\x7F]" <files>`.

Keep source lines within 100 characters — the linters enforce this for Scala, Java, and Python, and LLMs commonly overrun it in comments and long expressions. A quick scan of just the changed files catches most cases in seconds, far cheaper than a CI round trip:

    { git diff --name-only --diff-filter=ACM HEAD; git ls-files --others --exclude-standard; } \
      | grep -E '\.(scala|java|py)$' | sort -u \
      | xargs -r awk 'length>100 && $0 !~ /^[[:space:]]*(import|package) / && $0 !~ /https?:\/\// \
          {print FILENAME":"FNR": "length" chars"}'

This is only a hint: it approximates the linters' exemptions (imports, URLs) rather than matching them exactly, so it can over- or under-report. The linters remain the source of truth.

## Scala Test Base Classes

When writing a new Scala test suite, pick the lowest base class that provides what the test actually needs. Spark uses the `AnyFunSuite` ScalaTest style throughout, so the bases below are the chain to choose from. Each adds capability on top of the previous:

    SparkFunSuite                                                           (core)
      <- PlanTest                                                           (sql/catalyst)
        <- QueryTest                                                        (sql/core)

| Test scope | Base | Notes |
|------------|------|-------|
| Plain JVM/Scala — no Spark SQL | `SparkFunSuite` | `core` utilities, RDD, network, util classes, etc. Adds per-test timeout, `testRetry`, `gridTest`, thread audit, fixed timezone/locale, `withTempDir`, `withLogAppender`, `checkError`. |
| Catalyst plan tests — no `SparkSession` | `PlanTest` | Adds `comparePlans`, `normalizePlan`, `normalizeExprIds`. For analyzer / optimizer / planner rule tests. |
| SQL/DataFrame tests — needs a `SparkSession` | `QueryTest` | Adds `checkAnswer`, codegen-on/off helpers. `spark: SparkSession` is abstract and must be supplied by a session-providing trait (see below). |

### Providing a `SparkSession` for `QueryTest`

`QueryTest` declares `spark: SparkSession` abstractly via `SparkSessionProvider`, so it cannot be instantiated on its own. A concrete suite mixes in one of the session-providing traits below:

    QueryTest                                                               (abstract `spark`)
      + SharedSparkSession (sql/core)        -> classic in-process `TestSparkSession`
      + TestHiveSingleton  (sql/hive)        -> Hive-backed `TestHive` session

| Session provider | Module / location | Typical usage |
|---|---|---|
| `SharedSparkSession` | `sql/core` | Already extends `QueryTest` for historical reasons, but still mix in `QueryTest` explicitly, e.g. `class X extends QueryTest with SharedSparkSession`. Default for tests under `sql/core`. |
| `TestHiveSingleton` | `sql/hive` | Mixed in alongside `QueryTest`, e.g. `class X extends QueryTest with TestHiveSingleton`. Used by tests under `sql/hive`. |

## Python Test Base Classes

PySpark tests use the stdlib `unittest` framework: every suite subclasses `unittest.TestCase` (run via `python/run-tests`, see below). As with Scala, pick the lowest base that provides what the test actually needs. The bases live under `python/pyspark/testing/` and each adds capability on top of the previous:

    unittest.TestCase                                     (stdlib)
      <- PySparkBaseTestCase                              (pyspark.testing.utils)
        <- ReusedPySparkTestCase                          (pyspark.testing.utils)
          <- ReusedSQLTestCase                            (pyspark.testing.sqlutils)
            <- PandasOnSparkTestCase                      (pyspark.testing.pandasutils)

| Test scope | Base | Notes |
|------------|------|-------|
| Plain Python — no Spark | `unittest.TestCase` | Use the stdlib base directly. `PySparkBaseTestCase` is the same thing plus a SIGTERM fault-handler dump enabled when `PYSPARK_TEST_TIMEOUT` is set; subclass it only when you want that. |
| RDD / `SparkContext` — no `SparkSession` | `PySparkTestCase` (fresh) or `ReusedPySparkTestCase` (shared) | Both create a `SparkContext("local[4]")`. `PySparkTestCase` makes a new one per test (isolation); `ReusedPySparkTestCase` shares one per class (faster, the usual choice) and adds `quiet()` and an overridable `conf()` / `master()`. |
| SQL / DataFrame — needs a `SparkSession` | `ReusedSQLTestCase` | The workhorse for classic tests under `python/pyspark/sql`. Adds a shared `cls.spark`, sample `cls.df` / `cls.testData`, and mixes in `SQLTestUtils` + `PySparkErrorTestUtils`. Classic (non-Connect) mode. |
| pandas API on Spark | `PandasOnSparkTestCase` | Extends `ReusedSQLTestCase` with Arrow enabled and pandas-on-Spark assertions (`PandasOnSparkTestUtils`); `ComparisonTestBase` builds on it. |

### Spark Connect test bases

Spark Connect suites live in `pyspark.testing.connectutils` and are auto-skipped (via `should_test_connect`) when Connect dependencies are missing:

    PySparkBaseTestCase                                   (pyspark.testing.utils)
      <- PlanOnlyTestFixture                              (pyspark.testing.connectutils)
      <- ReusedConnectTestCase                            (pyspark.testing.connectutils)
           <- ReusedMixedTestCase                         (pyspark.testing.connectutils)

| Test scope | Base | Notes |
|------------|------|-------|
| Plan / proto construction — no server | `PlanOnlyTestFixture` | Uses a `MockRemoteSession`; builds and inspects plans without a running Connect server. For proto / plan-shape assertions. |
| Connect DataFrame — real session | `ReusedConnectTestCase` | The Connect analog of `ReusedSQLTestCase`; starts a session via `.remote(...)` (honoring `SPARK_CONNECT_TESTING_REMOTE`, default `local[4]`). Mixes in `SQLTestUtils` + `PySparkErrorTestUtils`. |
| Classic + Connect side by side | `ReusedMixedTestCase` | Extends `ReusedConnectTestCase`. For directly comparing classic vs Connect: it exposes a classic `self.spark` and a Connect `self.connect` so a test can run the same operation on each and assert they agree (`compare_by_show`, `both_conf`). Requires JVM access. |

### Mixins and helpers

These are combined with a base above rather than used on their own:

- `SQLTestUtils` — context managers `sql_conf`, `table`, `temp_view`, `view`, `database`, `function`, `temp_func`, `temp_env`; assumes `self.spark`. Already mixed into `ReusedSQLTestCase` and `ReusedConnectTestCase`.
- `PySparkErrorTestUtils` — `check_error(...)` to assert on a `PySparkException`'s error class and message parameters. The Python counterpart of Scala's `checkError`.
- `assertDataFrameEqual` / `assertSchemaEqual` (public API, from `pyspark.testing`) — standalone assertion functions that work in any test, no particular base class required.
- Domain bases outside the main ladder: `SparkSessionTestCase` (`pyspark.testing.mlutils`, for `ml`), `MLlibTestCase` (`pyspark.testing.mllibutils`), and `PySparkStreamingTestCase` (`pyspark.testing.streamingutils`, DStreams).

## Build and Test

Build and tests can take a long time. If the user explicitly asked to run tests, run them. Otherwise (you are running tests on your own to verify a change), first ask the user if they have more changes to make.

Prefer SBT over Maven for faster incremental compilation. Module names are defined in `project/SparkBuild.scala`.

Compile a single module:

    build/sbt <module>/compile

Compile test code for a single module:

    build/sbt <module>/Test/compile

Run test suites by wildcard or full class name:

    build/sbt '<module>/testOnly *MySuite'
    build/sbt '<module>/testOnly org.apache.spark.sql.MySuite'

Run test cases matching a substring:

    build/sbt '<module>/testOnly *MySuite -- -z "test name"'

Run test cases in an optional module:

    build/sbt -P<maven-profiles> '<module>/testOnly *MySuite'

For faster iteration, keep SBT open in interactive mode:

    build/sbt
    > project <module>
    > testOnly *MySuite

### PySpark Tests

PySpark tests require building Spark with Hive support first:

    build/sbt -Phive package

Activate the virtual environment specified by the user, or default to `.venv`:

    source <venv>/bin/activate

If the default venv does not exist, create it:

    python3 -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install --group dev

Run a single test suite:

    python/run-tests --testnames pyspark.sql.tests.arrow.test_arrow

Run a single test case:

    python/run-tests --testnames "pyspark.sql.tests.test_catalog CatalogTests.test_current_database"

## Investigating PR CI Failures

Enumerate all failing check runs first, then drill into each by type. Do not assume a single failure: a PR can fail tests, linters, and the build at once, and these surface through different channels.

Step 1 — Get the fork owner and the latest commit SHA of the PR:

    gh api repos/apache/spark/pulls/<PR_NUMBER> --jq '{owner: .head.repo.owner.login, sha: .head.sha}'

Step 2 — List every failing check run on the fork's commit. This is the complete failure set:

    gh api repos/<OWNER>/spark/commits/<SHA>/check-runs --paginate \
      --jq '.check_runs[] | select(.conclusion == "failure") | {name, id: .id}'

A passing (or absent) "Report test results" does NOT mean CI is green. That check aggregates only test-case failures; linter, license, dependency, MiMa, compile, and doc-build failures are separate check runs that produce no test annotations. Always work from the list in Step 2, not from any single check.

Step 3 — Drill into each failure according to its kind:

- **Test jobs** (e.g. "Report test results", "Build modules: ..."): fetch failure annotations. Each annotation contains the test class, test name, and failure message:

      gh api repos/<OWNER>/spark/check-runs/<CHECK_RUN_ID>/annotations

- **Non-test jobs** (e.g. "Linters, licenses, and dependencies", "Build"): find the failed step, then read only that job's log:

      gh api repos/<OWNER>/spark/actions/jobs/<JOB_ID> \
        --jq '{name, steps: [.steps[] | select(.conclusion == "failure") | .name]}'
      gh api repos/<OWNER>/spark/actions/jobs/<JOB_ID>/logs

Avoid downloading the large per-shard *test* job logs — they are very large and slow; use the annotations for those. Lint, license, dependency, and build job logs are small and fine to read directly when a step fails.

## Checking PR Merge Status

Spark merges PRs with `dev/merge_spark_pr.py`, not the GitHub merge button, so a **merged PR shows up on GitHub as Closed, not Merged** — its `merged` / `mergedAt` are empty, and backports to maintenance branches are plain pushes. **Do not read a Closed PR as rejected**; most Closed PRs were in fact merged.

To check whether, and to which branches, a PR was merged, run `dev/pr_merge_status.py <pr-number>`:

    $ dev/pr_merge_status.py 56356
    PR #56356 (base master): [SPARK-57295][SQL] Make database location validation ...
    merged: yes
      master       9357bc9ae05
      branch-4.x   72edddb358e

It lists `master` and the latest major's release branches the commit reached (e.g. `branch-4.x`, `branch-4.2`) — or reports `open` or `closed without merging`. It needs `gh` authenticated and a non-shallow checkout, and exits non-zero with a clear message if the PR is unknown (404) or the environment isn't ready (no `gh`/auth, no apache/spark remote). Older majors' branches are omitted; pass `--all-branches` to list every branch the commit reached.

## Pull Request Workflow

PR title format is `[SPARK-xxxx][COMPONENT] Title`. Draft, WIP, MINOR, and TRIVIAL PRs may omit the JIRA ID. The component tag is derived from the JIRA component name: take the last word and uppercase it (e.g. `Project Infra` → `[INFRA]`, `Spark Core` → `[CORE]`, `Structured Streaming` → `[STREAMING]`, `SQL` → `[SQL]`).

Infer the PR title from the changes. If no ticket ID is given and the PR is not draft, WIP, MINOR, or TRIVIAL, create one using `dev/create_spark_jira.py`, using the PR title (without the JIRA ID and component tag) as the ticket title.

    python3 dev/create_spark_jira.py "<title>" -c <component> { -t <type> | -p <parent-jira-id> }

- **Component** (`-c`): the exact JIRA component name (not the PR title shorthand), e.g. "SQL", "Spark Core", "PySpark", "Connect". Run `python3 dev/create_spark_jira.py --list-components` for the full list.
- **Issue type** (`-t`): "Bug", "Improvement", "New Feature", "Test", "Documentation", or "Dependency upgrade".
- **Parent** (`-p`): if the user mentions a parent JIRA ticket (e.g., "this is a subtask of SPARK-12345"), pass it instead of `-t`. The issue type is automatically "Sub-task".

The script sets the latest unreleased version as the default affected version.

After creating a JIRA ticket, print a prominent notice so the user does not miss it:

    ============================================================
    JIRA ticket created: SPARK-XXXXX
    https://issues.apache.org/jira/browse/SPARK-XXXXX

    Title:              <title>
    Component(s):       <component>
    Issue type:         <type>
    Affected version(s): <version>
    Priority:           <priority>

    Please review and adjust these fields if needed.
    ============================================================

Before writing the PR description, read `.github/PULL_REQUEST_TEMPLATE` and fill in every section from that file.

DO NOT push to the upstream repo. Always push to the personal fork. Open PRs against `master` on the upstream repo.

DO NOT force push or use `--amend` on pushed commits unless the user explicitly asks. If the remote branch has new commits, fetch and rebase before pushing.

Always get user approval before external operations such as pushing commits, creating PRs, or posting comments. Use `gh pr create` to open PRs. If `gh` is not installed, generate the GitHub PR URL for the user and recommend installing the GitHub CLI.

## Versioning and Branch Policy

When a change needs a version — `@since` annotations, config `.version("...")` (`SQLConf` / `*Conf`), new `MimaExcludes` sections, etc. — use the version of the branch it first ships in, with `-SNAPSHOT` stripped. Determine that branch:

- **PR opened against a non-`master` base branch** (e.g. a maintenance line like `branch-4.2`): use that base branch's version -- when you're checked out on it, that's just the working tree's `pom.xml`. The helper below covers only the common `master`-base case.
- **PR opened against `master`:** most PRs merge to **both** `master` and the latest `branch-<N>.x` (the branch for the next feature release, e.g. `branch-4.x`), so use the `branch-<N>.x` version. The exception is **master-only** changes — use `master`'s version — which are only:
  - breaking / binary-incompatible changes that can't ship in a minor release;
  - dependency upgrades that don't fix a critical issue worth backporting.

Do **not** just read `master`'s version: a normally-backported PR ships first in `branch-<N>.x`, whose version is lower than `master`'s. If unsure whether a change is master-only, ask the user.

`dev/next_version_candidates.py` (no arguments) prints both candidate versions, reading from the local `apache/spark` remote (the `upstream` configured during pre-flight). It reports the mechanical facts only -- choosing between them per the rules above is the judgement call (the numbers below are illustrative and advance over time):

    $ dev/next_version_candidates.py
    master       5.0.0
    branch-4.x   4.3.0

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository should consult `SECURITY.md` for the project's threat model, in-scope / out-of-scope declarations, and known non-findings before reporting issues.
