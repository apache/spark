# Apache Spark

Apache Spark is a multi-language engine for large-scale data processing and analytics, primarily written in Scala and Java. It provides SQL and DataFrame APIs for both batch and streaming workloads, with Spark Connect as an optional server-client protocol.

## Before Making Changes

Before the first edit in a session, check the git state:
1. If there are uncommitted changes, ask the user whether to continue editing or stash/commit first.
2. If the branch is `master`, or has new commits compared to `master` (check with `git log master..HEAD`), create a new branch from `master` before editing. Inform the user of the new branch name.
3. Otherwise, proceed on the current branch.

## Development Notes

SQL golden file tests are managed by `SQLQueryTestSuite` and its variants. Read the class documentation before running or updating these tests.

Spark Connect protocol is defined in proto files under `sql/connect/common/src/main/protobuf/`. Read the README there before modifying proto definitions.

## Build and Test

Prefer SBT over Maven for day-to-day development (faster incremental compilation). Replace `sql` below with the target module (e.g., `core`, `catalyst`, `connect`).

Compile a single module:

    build/sbt sql/compile

Compile test code for a single module:

    build/sbt sql/Test/compile

Run test suites by wildcard or full class name:

    build/sbt "sql/testOnly *MySuite"
    build/sbt "sql/testOnly org.apache.spark.sql.MySuite"

Run test cases matching a substring:

    build/sbt "sql/testOnly *MySuite -- -z \"test name\""

For faster iteration, keep SBT open in interactive mode:

    build/sbt
    > project sql
    > testOnly *MySuite

### PySpark Tests

PySpark tests require building Spark with Hive support first:

    build/sbt -Phive package

Set up and activate a virtual environment:

    if [ ! -d .venv ]; then
        python3 -m venv .venv
        source .venv/bin/activate
        pip install -r dev/requirements.txt
    else
        source .venv/bin/activate
    fi

Run a single test suite:

    python/run-tests --testnames pyspark.sql.tests.arrow.test_arrow

Run a single test case:

    python/run-tests --testnames "pyspark.sql.tests.test_catalog CatalogTests.test_current_database"

## Pull Request Workflow

### PR Title

Format: `[SPARK-xxxx][COMPONENT] Title`.
For follow-up fixes: `[SPARK-xxxx][COMPONENT][FOLLOWUP] Title`.

### PR Description

Follow the template in `.github/PULL_REQUEST_TEMPLATE`.

### GitHub Workflow

Contributors push their feature branch to their personal fork and open PRs against `master` on the upstream Apache Spark repo. Run `git remote -v` to identify which remote is the fork and which is upstream (`apache/spark`). If the remotes are unclear, ask the user to set them up following the standard convention (`origin` for the fork, `upstream` for `apache/spark`).

Use `gh pr create` to open PRs. If `gh` is not installed, generate the GitHub PR URL for the user and recommend installing the GitHub CLI.
