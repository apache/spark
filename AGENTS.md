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

## Test Base Classes

When writing a new Scala test suite, pick the lowest base class that provides what the test actually needs. The chain is layered — each adds capability on top of the previous:

    SparkTestSuite                                                          (core; style-agnostic foundation)
      <- SparkFunSuite = AnyFunSuite + SparkTestSuite                       (core; pins the FunSuite style — the default)
        <- PlanTest = SparkFunSuite + PlanTestBase                          (sql/catalyst)
        <- QueryTest = SparkFunSuite + QueryTestBase + PlanTest             (sql/core)
          <- SharedSparkSession = QueryTest + SharedSparkSessionBase        (sql/core)

| Test scope | Base | Notes |
|------------|------|-------|
| Non-FunSuite ScalaTest style (rare) | `SparkTestSuite` | Style-agnostic trait holding the common Spark test functionality (thread audit, fixed timezone/locale, `withTempDir`, `withLogAppender`, `checkError`, etc.). Mix with any ScalaTest style — see `core/src/test/scala/org/apache/spark/{WordSpec,FunSpec,FlatSpec,...}SparkTestSuite.scala` for examples. Real Spark tests should use `SparkFunSuite` instead. |
| Plain JVM/Scala — no Spark SQL | `SparkFunSuite` | `core` utilities, RDD, network, util classes, etc. = `AnyFunSuite + SparkTestSuite`. Adds per-test timeout, `testRetry`, `gridTest` on top of `SparkTestSuite`. |
| Catalyst plan tests — no `SparkSession` | `PlanTest` | Adds `comparePlans`, `normalizePlan`, `normalizeExprIds`. For analyzer / optimizer / planner rule tests. |
| SQL/DataFrame helpers — abstract `spark` | `QueryTest` | Adds `checkAnswer`, codegen-on/off helpers. Cannot be instantiated alone — `spark` is abstract and must be supplied by a session-providing trait. |
| SQL/DataFrame integration tests — provides a session | `SharedSparkSession` | The default for most SQL suites. Provides a shared classic `TestSparkSession`, `testImplicits`, plus `checkAnswer` from `QueryTest`. |

`QueryTest` declares `spark: SparkSession` abstractly via `SparkSessionProvider`. To run a concrete suite, mix in a session-providing trait. The common providers in this repo are:

| Session provider | Module / location | Base chain | Use case |
|---|---|---|---|
| `SharedSparkSession` | `sql/core` | `QueryTest` -> `SparkFunSuite` | Classic in-process `SparkSession`. Default for tests under `sql/core`. |
| `TestHiveSingleton` | `sql/hive` | `SparkFunSuite` | Hive-backed session (`TestHive`). Used by tests under `sql/hive`. |
| `RemoteSparkSession` | `sql/connect/client/jvm` | `AnyFunSuite` (NOT `SparkFunSuite`) | Spark Connect client session. Used by Connect client tests. |

`RemoteSparkSession` is the odd one out: it directly extends `AnyFunSuite` (with `// scalastyle:ignore funsuite`) instead of going through `SparkFunSuite`, so on its own it lacks SparkFunSuite-only features (per-test timeout, `gridTest`, `retry`, log-capture). Connect client suites therefore combine it with `QueryTest` (e.g. `class DataFrameSuite extends QueryTest with RemoteSparkSession`) to bring the `SparkFunSuite` chain back in.

A handful of suites (e.g. `MapStatusEndToEndSuite`, `ExecutorSideSQLConfSuite`) override `spark` directly and manage the session lifecycle themselves; reach for that pattern only when none of the providers above fit (e.g. local-cluster mode, custom builder configs).

Helper traits like `ParquetTest`, `OrcTest`, `FileBasedDataSourceTest`, `DDLCommandTestUtils` already extend `QueryTest` but do NOT supply a session. Combine them with one of the session providers above (e.g. `extends ParquetTest with SharedSparkSession`, `extends QueryTest with TestHiveSingleton`).

Common mistakes in the `extends` clause:
- `extends QueryTest with SharedSparkSession` — `QueryTest` is redundant (`SharedSparkSession` already extends it). Use `extends SharedSparkSession`.
- `extends QueryTest with ParquetTest` / `with FileBasedDataSourceTest` / `with OrcTest` — `QueryTest` is redundant; these helper traits all extend `QueryTest`.
- `extends OrcTest with QueryTestBase` — `OrcTest -> QueryTest -> QueryTestBase` already.
- `extends QueryTest with CommandSuiteBase with DDLCommandTestUtils` — both `CommandSuiteBase` (via `SharedSparkSession`) and `DDLCommandTestUtils` extend `QueryTest`, so `QueryTest` is redundant.
- `extends ParquetTest with SharedSparkSession` is NOT redundant — the format helper trait and the session trait bring different things.

Linearization gotcha: the first item in the `extends` clause must transitively extend a class (i.e. carry a non-`Object` superclass). Of the bases above, `SparkFunSuite`, `PlanTest`, `QueryTest`, and `SharedSparkSession` all carry the `SparkFunSuite` -> `AnyFunSuite` chain. `SparkTestSuite` is a pure trait and does NOT — if you use it directly you must put a ScalaTest style class first (e.g. `class X extends AnyWordSpec with SparkTestSuite`). The same applies to other "pure helper" traits (`*ErrorsBase`, `*Helper`): if you put one first, mix in a class-bearing trait immediately after, or compilation fails with `superclass Object is not a subclass of the superclass SparkFunSuite of the mixin trait ...`. Quick check: `grep "^trait <Name>"` — if it ends in `extends DataTypeErrorsBase` or another pure trait, it does not carry the class chain.

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
    pip install -r dev/requirements.txt

Run a single test suite:

    python/run-tests --testnames pyspark.sql.tests.arrow.test_arrow

Run a single test case:

    python/run-tests --testnames "pyspark.sql.tests.test_catalog CatalogTests.test_current_database"

## Investigating PR CI Failures

Do NOT download full job logs to grep for errors — they are very large and slow. Instead, use the test report annotations on the fork.

Step 1 — Get the fork owner and the latest commit SHA of the PR:

    gh api repos/apache/spark/pulls/<PR_NUMBER> --jq '{owner: .head.repo.owner.login, sha: .head.sha}'

Step 2 — Find the "Report test results" check run on the fork's commit:

    gh api repos/<OWNER>/spark/commits/<SHA>/check-runs \
      --jq '.check_runs[] | select(.name == "Report test results") | {id: .id, annotations: .output.annotations_count}'

Step 3 — Fetch failure annotations:

    gh api repos/<OWNER>/spark/check-runs/<CHECK_RUN_ID>/annotations

Each annotation contains the test class, test name, and failure message.

## Pull Request Workflow

PR title format is `[SPARK-xxxx][COMPONENT] Title`. The component tag is derived from the JIRA component name: take the last word and uppercase it (e.g. `Project Infra` → `[INFRA]`, `Spark Core` → `[CORE]`, `Structured Streaming` → `[STREAMING]`, `SQL` → `[SQL]`).

Infer the PR title from the changes. If no ticket ID is given, create one using `dev/create_spark_jira.py`, using the PR title (without the JIRA ID and component tag) as the ticket title.

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
