# Investigating PR CI Failures

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
