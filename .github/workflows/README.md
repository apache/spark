# Continuous Integration on GitHub Actions

The continuous integration build is critical to the project's health and development velocity. It enables contributors to make changes to Spark with the confidence that they have not inadvertently broken anything.

There are several considerations to keep in mind when making changes to the build.

## Resource usage limits

The Apache Software Foundation (ASF) imposes [resource usage limits][limit] on GitHub Actions, including on the number of concurrent running jobs as well as on the total number of [run minutes used][usage] per week and rolling five-day window. When we hit these limits it can cause jobs to queue up, bringing development to a crawl.

[limit]: https://infra.apache.org/github-actions-policy.html
[usage]: https://github.com/apache/spark/actions/metrics/usage

### Reduce and stagger scheduled workloads

Use daily, three-day, or weekly cadences for scheduled workloads. Move two-day
workloads to three days and four- or six-day workloads to weekly. Keep Pages,
stale-PR cleanup, and the base-image cache daily. The build-status updater
keeps its separate schedule.

Reserve these UTC hours for workloads outside the three-day cadence:

| UTC hour | Workloads |
| --- | --- |
| 00 | Daily stale-PR cleanup at 00:30 |
| 04 | Daily base-image cache at 04:17, alongside weekly variants |
| 08, 20 | Weekly variants at minute 17 |
| 11 | Weekly branch-3.5 scheduler on Sunday at 11:00 |
| 12 | Daily Pages at 12:30 |

Three-day workloads use the remaining 18 hours, with dates offset across three
phases: `1-31/3`, `2-31/3`, and `3-31/3`. These day-of-month phases restart each
month, so their gaps can be shorter at month boundaries. Weekly workloads use
weekday crons, such as `17 8 * * MON`, to keep seven-day intervals across months.
Their configured starts are:

| Weekday | UTC | Workload | Branch |
| --- | --- | --- | --- |
| Monday | 04:17 | [Maven macOS](build_maven_java21_macos26.yml) | master |
| Monday | 20:17 | [Python no-GIL](build_python_3.14_nogil.yml) | master |
| Tuesday | 08:17 | [Python 3.11](build_python_3.11.yml) | branch-4.0 |
| Tuesday | 20:17 | [Unix domain socket](build_uds.yml) | master |
| Wednesday | 08:17 | [Java 21](build_java21.yml) | branch-4.0 |
| Wednesday | 20:17 | [Python classic](build_python_3.12_classic_only.yml) | master |
| Thursday | 04:17 | [Maven Java 21](build_maven_java21.yml) | branch-4.0 |
| Thursday | 08:17 | [Non-ANSI](build_non_ansi.yml) | branch-4.0 |
| Friday | 04:17 | [Maven ARM](build_maven_java21_arm.yml) | master |
| Friday | 20:17 | [Python ARM](build_python_3.12_arm.yml) | master |
| Saturday | 04:17 | [Maven](build_maven.yml) | branch-4.0 |
| Saturday | 08:17 | [Java 17](build_java17.yml) | branch-4.0 |
| Saturday | 20:17 | [PyPy 3.10](build_python_pypy3.10.yml) | branch-4.0 |
| Sunday | 11:00 | [Python 3.9](build_python_3.9.yml) | branch-3.5 |
| Sunday | 11:00 | [Java 8 / Scala 2.13](build_scala213.yml) | branch-3.5 |

The branch-3.5 scheduler dispatches both variants from one shared weekly trigger.
Use minute 17 for rescheduled builds to avoid GitHub Actions' busy
[start-of-hour scheduling period][schedule-event]. The minute is an arbitrary
off-hour choice; weekday, hour, and phase assignments distribute load without
ranking jobs by importance. An individual interval can be shortened later if
faster feedback is justified.

Some overlap is allowed, including the daily cache and weekly builds at 04:17.
Scheduled starts can be delayed, and running workflows can overlap even when
their configured starts differ.

Maintenance-branch schedulers run on the default branch and dispatch workflows
to their target branches. When changing a branch schedule, update both its
`on.schedule` cron and the matching `github.event.schedule` condition. Preserve
the skip-if-unchanged check. Check existing schedules when assigning slots, and
keep three-day workloads out of the reserved hours. Other event triggers are
unaffected.

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
