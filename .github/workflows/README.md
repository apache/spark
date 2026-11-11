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

### CI dashboard

The [CI overview](https://apache.github.io/spark/ci-dashboard.html) shows measured CI burden for past days and estimates for future days in one monthly UTC timeline. Blue cells use actual upstream Actions job timestamps across push, scheduled, manual, and other workflow triggers, including failures, reruns, and running-job execution so far. Orange cells combine scheduled job profiles with recent upstream push usage averaged by weekday and UTC hour. Today is measured through the collection cutoff; unavailable history stays gray. Actual and forecast totals are reported separately, including per-branch totals. Fork usage and frequent Build Status polling are outside this build timeline.

The existing Pages deployment starts with three measured days on a cold cache, expands to seven on a warm cache, and retains up to 28 days of observed history. It reuses public completed-job timings and invalidates them when a run is updated or rerun. The Actions cache contains timestamps and job metadata, never credentials. Scheduled profiles average up to three successful first-attempt runs per target from the preceding 28 days, partitioning SNAPSHOT publishing by branch. Forecasts spread jobs across their execution hours and include cross-hour and cross-month overlap. Profile coverage and the push baseline's observed-day count are displayed; unprofiled targets and future manual runs remain unmodeled. Runner-hours are execution time without billing or OS multipliers.

Normal documentation and PR builds make no history requests and show configured starts when timings are absent. To preview with PyYAML installed, run `python3 dev/generate-ci-dashboard.py --output docs/ci-dashboard-data.json`, serve `docs/` over HTTP, and open `ci-dashboard.html`. Add `--history --history-cache /tmp/ci-history.json` with `GH_TOKEN` or `GITHUB_TOKEN` set to collect and retain timings. Generated metadata is not committed. The existing documentation CI job runs `python3 -B -m unittest discover -s dev/tests -p 'test_ci_dashboard*.py'` and `node --test dev/test-ci-dashboard.js`.

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

## Composite actions

If you find yourself repeating the same set of instructions across multiple jobs, extract them into a reusable [composite action] under [`actions/`](../actions/).

[composite action]: https://docs.github.com/en/actions/tutorials/create-actions/create-a-composite-action

## Security considerations

Keep [these security considerations][security] in mind when authoring workflows, as it's possible for malicious forks to use poorly written workflows to steal secrets or merge unapproved code.

[security]: https://cwiki.apache.org/confluence/spaces/BUILDS/pages/321719166/GitHub+Actions+Security
