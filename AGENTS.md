# Apache Spark

## Agent Instructions in This Repo

Each `AGENTS.md` is an index: what every task in its scope needs, plus pointers to detail in a
`.agents/` directory beside it. When a pointer's condition matches your task, read the file it
names before acting.

When exploring or working in any directory, read and follow every `AGENTS.md` in that directory
and its ancestors up to the repo root; instructions in a more specific directory take precedence
for that directory's scope. A directory may carry these instructions as `CLAUDE.md` rather than
`AGENTS.md`, so check **both** names. Where both exist, one is a symlink to the other, and reading
either one is sufficient.

Agent instructions go in `AGENTS.md` or the `.agents/` directory beside it. Before adding or
editing them: read `.agents/agent-docs.md`.

## Pre-flight Checks

Before the first code edit or running test in a session, ensure a clean working environment. DO NOT skip these checks:

1. Run `git remote -v` to identify the personal fork and upstream (`apache/spark`). If unclear, ask the user to configure their remotes following the standard convention (`origin` for the fork, `upstream` for `apache/spark`).
2. If the latest commit on `<upstream>/master` is more than a day old (check with `git log -1 --format="%ci" <upstream>/master`), run `git fetch <upstream> master`.
3. If there are uncommitted changes (check with `git status`), ask the user to stash them before proceeding.
4. Switch to the appropriate branch:
   - **Existing PR**: resolve the PR branch name via `gh api repos/apache/spark/pulls/<number> --jq '.head.ref'`, then look for a local branch matching that name. If found, switch to it and inform the user. If not found, ask whether to fetch it or if there is a local branch under a different name.
   - **New edits**: ask the user to choose: create a new git worktree from `<upstream>/master` with `--no-track` and work from there (recommended), or create and switch to a new branch from `<upstream>/master` with `--no-track`.
   - **Running tests**: use `<upstream>/master`.

## Build and Test

- Build and tests can take a long time, so prefer the narrowest relevant command. Prefer SBT over
  Maven; module names are defined in `project/SparkBuild.scala`. Compile with
  `build/sbt <module>/compile` (`<module>/Test/compile` for test code), and run a suite or test
  case with `build/sbt '<module>/testOnly *MySuite -- -z "test name"'`.
- Before running tests in an optional module, using SBT interactively, or running PySpark tests:
  read `.agents/build-and-test.md`. For setup and local troubleshooting: `docs/building-spark.md`.
- Before writing a new Scala or Python test suite: read `.agents/test-base-classes.md` and pick
  the lowest base class that provides what the test actually needs.
- When writing or changing a test, pin the configs its assertions depend on: runtime SQL configs
  with `withSQLConf` (`self.sql_conf` in Python), static SQL and core configs in the suite's conf
  (`sparkConf` in `SharedSparkSession` suites, `conf` in `SharedSparkContext` suites, `conf()` in
  Python). Scheduled jobs rerun the suites with other defaults set through env vars: Non-ANSI
  (`SPARK_ANSI_SQL_MODE=false`), UDS (`PYSPARK_UDS_MODE=true`), JDK codegen
  (`SPARK_CODEGEN_COMPILER=jdk`) and RocksDB UI (`LIVE_UI_LOCAL_STORE_DIR=<dir>`). The RocksDB UI
  job keeps the live UI store on disk, and no config switches it back to memory, so a UI or
  listener test must pass with either store. When a result differs by ANSI mode and both matter,
  assert both modes instead of pinning one. To check, rerun the suite with the job's env var set.
- Never remove, skip or weaken a failing test (`ignore`, `assume(false)`, `@unittest.skip`, or an
  expected value or golden file updated just to match), or mock the code under test, unless asked.
  Explain each new mock in a code comment.
- SQL golden file tests are managed by `SQLQueryTestSuite` and its variants. Read the class
  documentation before running or updating these tests. DO NOT edit the generated golden files
  (`.sql.out`) directly. Always regenerate them when needed, and carefully review the diff to make
  sure it's expected.
- When adding new SQL syntax or a user-visible SQL feature, follow the streaming test coverage
  guidance in the `SQLQueryTestSuite` class documentation.

## Code Standards

- Keep source lines within 100 characters; the linters enforce this for Scala, Java, and Python.
  Avoid non-ASCII characters in code and comments (string literals may contain them when the
  content requires it). When changing Scala, Java or Python code: read
  `.agents/code-conventions.md`, and run its spot-checks before committing.
- Every new text file that `dev/.rat-excludes` does not already match needs the Apache License
  header, copied from a neighboring file of the same type and placed after the `#!` line of an
  executable script; the license check in CI fails otherwise. If one cannot carry a header, such
  as test data, add a `dev/.rat-excludes` entry that matches no other file or directory name: RAT
  matches each entry against bare names anywhere in the tree. Binary files need neither.
- When adding a member to an existing class or object, put it with the code it belongs with,
  following the file's existing sectioning. Do not reorganize existing members unless the change
  requires it.
- Spark Connect protocol is defined in proto files under `sql/connect/common/src/main/protobuf/`.
  Read the README there before modifying proto definitions.
- A change that needs a version (`@since`, config `.version("...")`, a `MimaExcludes` entry): read
  `.agents/versioning.md` first. It is usually not `master`'s version.

## Pull Requests

- Before creating a JIRA ticket or a PR, or writing a PR title or description: read
  `.agents/pr-workflow.md`.
- DO NOT push to the upstream repo. Always push to the personal fork. Open PRs against `master` on
  the upstream repo.
- DO NOT force push or use `--amend` on pushed commits unless the user explicitly asks. If the
  remote branch has new commits, fetch and rebase before pushing.
- Always get user approval before external operations such as pushing commits, creating PRs, or
  posting comments. Use `gh pr create` to open PRs. If `gh` is not installed, generate the GitHub
  PR URL for the user and recommend installing the GitHub CLI.

## Investigations

Read the runbook before starting:

- A PR's CI failures: `.agents/pr-ci-failures.md`.
- Whether, or to which branches, a PR was merged: `.agents/pr-merge-status.md`. Spark merges PRs
  with `dev/merge_spark_pr.py`, so a merged PR shows up on GitHub as Closed, not Merged; do not
  read a Closed PR as rejected.

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository should consult `SECURITY.md` for the project's threat model, in-scope / out-of-scope declarations, and known non-findings before reporting issues.
