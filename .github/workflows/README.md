# Continuous Integration on GitHub Actions

The continuous integration build is critical to the project's health and development velocity. It enables contributors to make changes to Spark with the confidence that they have not inadvertently broken anything.

There are several considerations to keep in mind when making changes to the build.

## Resource usage limits

The Apache Software Foundation (ASF) imposes [resource usage limits][limit] on GitHub Actions, including on the number of concurrent running jobs as well as on the total number of [run minutes used][usage] per week and rolling five-day window. When we hit these limits it can cause jobs to queue up, bringing development to a crawl.

[limit]: https://infra.apache.org/github-actions-policy.html
[usage]: https://github.com/apache/spark/actions/metrics/usage

### Scheduled workflow cadence

Use daily maintenance schedules and three-day or weekly build schedules to limit runner consumption while retaining regular coverage. Reserve 04 and 12 UTC for daily workflows and 03, 09, 15, and 21 UTC for weekly workflows; distribute three-day workflows across the remaining hours and day-of-month phases, and spread weekly workflows across the days of the week. Balance expensive builds across phases and start times, and use minute 17 for rescheduled builds to avoid GitHub Actions' busy [start-of-hour scheduling period][schedule-event]; the specific minute is arbitrary. The build-status updater keeps its separate 15-minute schedule.

These assignments spread runner demand without ranking jobs by importance, and individual intervals can be shortened when faster feedback is needed. Day-of-month phases restart monthly, so intervals vary at month boundaries, and delayed starts or long runs can still overlap.

[schedule-event]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule

### Push heavy workflows out to forks

Workflows triggered by `on: pull_request` [count against the base repo's (i.e. this repo's) activity limits][base]. For this reason it's [important] to use `on: push` for heavy workflows. `on: push` workflows count against the fork's limits and not ours, enabling more jobs to run concurrently.

[base]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull-request-events-for-forked-repositories
[important]: https://github.com/apache/spark/pull/32092

#### Downsides of avoiding the `pull_request` workflow trigger

The [`pull_request` workflow trigger][prt] provides key benefits that make working with workflows easier, including:

[prt]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request

1. The workflow runs against the target repo's context (i.e. `apache/spark`).
2. GitHub provides a stable merge reference.
3. PR metadata is readily available via `github.event.pull_request`.

Without these benefits, some GitHub Actions patterns become more difficult or impossible to implement. We've implemented our own custom checkout pattern via the [`checkout-and-sync`](../actions/checkout-and-sync/) composite action.

If in the future GitHub somehow allows workflows to run `on: pull_request` while counting those jobs against the forks' limits instead of ours, that would help eliminate various distortions of our build caused by the move away from `on: pull_request`. We would get the best functionality from GitHub Actions without hitting job run limits.

## Documentation previews

Documentation builds on `apache/spark` upload the generated site as a
`docs-preview-<branch>-<sha>-<attempt>` artifact, retained for one day. The archive
is named `site-<sha>-<attempt>.tar.bz2`. Names include the checked-out commit hash
and workflow run attempt so reruns can upload without replacing existing artifacts.
Change-based skipping of Python API docs applies only to fork builds.

CI builds keep their source versions unchanged and export `SKIP_RDOC=1` before
the complete Jekyll build to skip R API documentation. The initial build without
API docs still checks that the documentation builds independently of generated
API docs.

The Pages workflow selects the unexpired master preview artifact with the newest
creation date, using its artifact ID to download that exact build. Commit hashes
do not determine artifact order. After extraction, Pages applies the `In-Progress`
Spark and PySpark version labels, search filter, and PySpark version-switcher
settings to the generated site. Pages runs daily or on manual dispatch without
rebuilding the documentation. It can use a completed documentation build even if
another job in the same CI run fails. If no preview artifact is available, Pages
fails and leaves the existing deployment in place.

## Composite actions

If you find yourself repeating the same set of instructions across multiple jobs, extract them into a reusable [composite action] under [`actions/`](../actions/).

[composite action]: https://docs.github.com/en/actions/tutorials/create-actions/create-a-composite-action

## Security considerations

Keep [these security considerations][security] in mind when authoring workflows, as it's possible for malicious forks to use poorly written workflows to steal secrets or merge unapproved code.

[security]: https://cwiki.apache.org/confluence/spaces/BUILDS/pages/321719166/GitHub+Actions+Security
