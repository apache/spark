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

## Build and Test

Build and tests can take a long time. Before running tests, ask the user if they have more changes to make.

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

### Doc-gen (unidoc) failures

The `Run / Documentation generation` job's log contains ~100 `[error]` lines from `target/java/.../X.java` files (genjavadoc-generated stubs). These look fatal but are intentionally non-fatal — `--ignore-source-errors` is set in `JavaUnidoc / unidoc / javacOptions`. They never cause exit 1. To find the actual cause:

1. **Filter `[error]` lines for paths NOT under `target/java/`.** The remaining 0–3 lines are the fatal ones — typically doclint violations like heading-out-of-sequence (`<H3>` after `<H1>`), malformed `@link`, missing `@param`. The PR-time diagnostic banner from `docs/_plugins/build_api_docs.rb` does this filtering automatically and prints the candidates.

2. **`Building tree → exit 1` is misleading.** It is NOT a tree-builder crash. The doclet typically completed all HTML generation before exiting 1 due to a non-zero error count. javadoc's `[done in N ms]` / `N errors / M warnings` summary confirms; a `Generating .../help-doc.html` line in the log means HTML generation finished.

3. **JVM-level instrumentation is rarely needed.** The filter in (1) almost always identifies the trigger. Reach for `-J-Xlog:exceptions=info`, `-J-Xlog:class+load=info`, or javadoc's own `-verbose` only as fallback, and be aware:
   - `-verbose` enables `[parsing X.java]` / `[loading Y.class]` / `Generating .../X.html` lines but produces ~1M log lines per failed run.
   - `-J-Xlog:exceptions=info` is dominated by JDK-internal noise (lambda machinery, resource bundles, javac's own `CompletionFailure` recovery). The visible exceptions are rarely the cause of exit 1.
   - `CompletionFailure` storms (same Throwable address re-thrown 100k+ times) are usually NOT the trigger — they are symptoms of unrelated downstream tooling (e.g. scaladoc's internal javac) running after the main javadoc has completed.

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
