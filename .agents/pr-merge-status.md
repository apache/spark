# Checking PR Merge Status

Spark merges PRs with `dev/merge_spark_pr.py`, not the GitHub merge button, so a **merged PR shows up on GitHub as Closed, not Merged** — its `merged` / `mergedAt` are empty, and backports to maintenance branches are plain pushes. **Do not read a Closed PR as rejected**; most Closed PRs were in fact merged.

To check whether, and to which branches, a PR was merged, run `dev/pr_merge_status.py <pr-number>`:

    $ dev/pr_merge_status.py 56356
    PR #56356 (base master): [SPARK-57295][SQL] Make database location validation ...
    merged: yes
      master       9357bc9ae05
      branch-4.x   72edddb358e

It lists `master` and the latest major's release branches the commit reached (e.g. `branch-4.x`, `branch-4.2`) — or reports `open` or `closed without merging`. It needs `gh` authenticated and a non-shallow checkout, and exits non-zero with a clear message if the PR is unknown (404) or the environment isn't ready (no `gh`/auth, no apache/spark remote). Older majors' branches are omitted; pass `--all-branches` to list every branch the commit reached.
