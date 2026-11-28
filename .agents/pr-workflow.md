# Pull Request Workflow

- PR title format is `[SPARK-xxxx][COMPONENT] Title`. Draft, WIP, MINOR, and TRIVIAL PRs may omit the JIRA ID. The component tag comes from the `COMPONENTS` registry in `dev/merge_spark_pr.py`, the source of truth: each entry pairs a PR-title tag with its canonical JIRA component name and accepted aliases (e.g. `Documentation` → `[DOC]`, `Pandas API on Spark` → `[PS]`, `SQL` → `[SQL]`). Read that registry to pick the right tag.
- Use `[FOLLOWUP]` only for small PRs that directly modify or correct unreleased earlier PRs. For separately planned work, non-trivial changes, or work outside the earlier JIRA's scope, create a separate JIRA ticket for each PR and use the normal title format without `[FOLLOWUP]`.

Infer the PR title from the changes. When creating a PR whose title needs a JIRA ID and none is given, take the earlier PR's ID for a `[FOLLOWUP]`; otherwise create a ticket using `dev/create_spark_jira.py`, with the PR title (without the JIRA ID and component tag) as the ticket title.

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
