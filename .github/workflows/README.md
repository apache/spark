# Continuous Integration on GitHub Actions

The continuous integration build is critical to the project's health and development velocity. It enables contributors to make changes to Spark with the confidence that they have not inadvertently broken anything.

There are several considerations to keep in mind when making changes to the build.

## Resource usage limits

The Apache Software Foundation (ASF) imposes [resource usage limits][limit] on GitHub Actions, including on the number of concurrent running jobs as well as on the total number of [run minutes used][usage] per week and rolling five-day window. When we hit these limits it can cause jobs to queue up, bringing development to a crawl.

[limit]: https://infra.apache.org/github-actions-policy.html
[usage]: https://github.com/apache/spark/actions/metrics/usage

### Reduce and stagger scheduled workloads

Move two-day workloads to a three-day cadence, but preserve existing four- and
six-day cadences. Do not shorten a workload's original interval when assigning
a new slot. The base-image cache moves from daily to three days.

Reserve these UTC hours for workloads outside the three-day cadence:

| UTC hour | Workloads |
| --- | --- |
| 00 | Daily stale-PR cleanup at 00:30 |
| 04, 08, 20 | Four- and six-day variants at minute 17 |
| 11 | Unchanged branch-3.5 scheduler at 11:00 |
| 12 | Daily Pages at 12:30 |

Three-day workloads use the remaining 18 hours, with dates offset across three
phases: `1-31/3`, `2-31/3`, and `3-31/3`. Longer cadences also use offsets, such
as `2-31/4` or `3-31/6`, to spread starts across their reserved hours.

Use minute 17 for rescheduled builds to avoid GitHub Actions' busy
[start-of-hour scheduling period][schedule-event]. The minute is an arbitrary
off-hour choice; hour and date assignments distribute load without ranking
jobs by importance. An individual interval can be shortened later if faster
feedback is justified.

Pages and stale-PR cleanup keep their daily frequencies. The branch-3.5
scheduler keeps its original shared four-day trigger. The build-status updater
keeps its separate schedule, and other event triggers are unaffected.

Day-of-month offsets restart each month, so these are calendar cadences rather
than strict rolling intervals. Some four- and six-day cycles intersect within
the reserved hours. Scheduled starts can be delayed, and running workflows can
overlap.

Maintenance-branch schedulers run on the default branch and dispatch workflows
to their target branches. When changing a branch schedule, update both its
`on.schedule` cron and the matching `github.event.schedule` condition. Preserve
the skip-if-unchanged check. Check existing schedules when assigning slots, and
keep three-day workloads out of the reserved hours.

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
