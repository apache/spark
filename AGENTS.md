# Apache Spark

## Before Making Changes

Before the first edit in a session, ensure a clean working environment. DO NOT skip these checks:

1. Run `git remote -v` to identify the personal fork and upstream (`apache/spark`). If unclear, ask the user to configure their remotes following the standard convention (`origin` for the fork, `upstream` for `apache/spark`).
2. If the latest commit on `<upstream>/master` is more than a day old (check with `git log -1 --format="%ci" <upstream>/master`), run `git fetch <upstream> master`.
3. If the current branch has uncommitted changes or commits not in upstream `master` (check with `git log <upstream>/master..HEAD`), suggest creating a new git worktree from `<upstream>/master` (recommended), or ask the user to clean up by creating a new branch from `<upstream>/master` or stashing changes.
4. Otherwise, proceed on the current branch.

## Development Notes

SQL golden file tests are managed by `SQLQueryTestSuite` and its variants. Read the class documentation before running or updating these tests. DO NOT edit the generated golden files (`.sql.out`) directly. Always regenerate them when needed, and carefully review the diff to make sure it's expected.

Spark Connect protocol is defined in proto files under `sql/connect/common/src/main/protobuf/`. Read the README there before modifying proto definitions.

## Build and Test

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

Activate the virtual environment specified by the user, or default to `~/.virtualenvs/pyspark`:

    source <venv>/bin/activate

If the default venv does not exist, create it:

    python3 -m venv ~/.virtualenvs/pyspark
    source ~/.virtualenvs/pyspark/bin/activate
    pip install -r dev/requirements.txt

Run a single test suite:

    python/run-tests --testnames pyspark.sql.tests.arrow.test_arrow

Run a single test case:

    python/run-tests --testnames "pyspark.sql.tests.test_catalog CatalogTests.test_current_database"

## Pull Request Workflow

PR title requires a JIRA ticket ID (e.g., `[SPARK-xxxx][SQL] Title`). Ask the user to create a new ticket or provide an existing one if not given. Follow the template in `.github/PULL_REQUEST_TEMPLATE` for the PR description.

DO NOT push to the upstream repo. Always push to the personal fork. Open PRs against `master` on the upstream repo.

Always get user approval before external operations such as pushing commits, creating PRs, or posting comments. Use `gh pr create` to open PRs. If `gh` is not installed, generate the GitHub PR URL for the user and recommend installing the GitHub CLI.
