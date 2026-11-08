# Continuous Integration on GitHub Actions

The continuous integration build is critical to the project's health and development velocity. It enables contributors to make changes to Spark with the confidence that they have not inadvertently broken anything.

There are several considerations to keep in mind when making changes to the build.

## Resource usage limits

The Apache Software Foundation (ASF) imposes [resource usage limits][limit] on GitHub Actions, including on the number of concurrent running jobs as well as on the total number of [run minutes used][usage] per week and rolling five-day window. When we hit these limits it can cause jobs to queue up, bringing development to a crawl.

[limit]: https://infra.apache.org/github-actions-policy.html
[usage]: https://github.com/apache/spark/actions/metrics/usage

### Spread scheduled workloads across three-day phases

Scheduled build variants use hourly UTC slots across three day-of-month phases:

| Phase | Dates | Cron day-of-month field |
| --- | --- | --- |
| 1 | 1, 4, 7, ... | `1-31/3` |
| 2 | 2, 5, 8, ... | `2-31/3` |
| 3 | 3, 6, 9, ... | `3-31/3` |

Use minute 17 for these staggered starts to avoid GitHub Actions' busy
[start-of-hour scheduling period][schedule-event]. The minute is an arbitrary
off-hour choice; the hour and date phase distribute the workloads, and minute
17 does not guarantee timely execution.

Pages retains its daily 12:30 UTC schedule, so the 12:00 hour is reserved for
Pages in every phase. Stale-PR cleanup keeps its daily frequency at 00:30 UTC.
The branch-3.5 scheduler retains its original four-day cadence and dispatches
both variants at 11:00 UTC. The other 66 workloads use the three-day cadence.
The build-status updater keeps its separate schedule. Other event triggers are
unaffected.

The phases restart each month, so intervals are approximately three days rather
than exactly 72 hours. Scheduled starts can be delayed, and running workflows
can overlap.

Maintenance-branch schedulers run on the default branch and dispatch workflows
to their target branches. When changing a branch schedule, update both its
`on.schedule` cron and the matching `github.event.schedule` condition. Preserve
the skip-if-unchanged check. When assigning a new slot, check the schedules in
all workflows to avoid starting another workload in the same phase and hour.

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

## Composite actions

If you find yourself repeating the same set of instructions across multiple jobs, extract them into a reusable [composite action] under [`actions/`](../actions/).

[composite action]: https://docs.github.com/en/actions/tutorials/create-actions/create-a-composite-action

## Security considerations

Keep [these security considerations][security] in mind when authoring workflows, as it's possible for malicious forks to use poorly written workflows to steal secrets or merge unapproved code.

[security]: https://cwiki.apache.org/confluence/spaces/BUILDS/pages/321719166/GitHub+Actions+Security
