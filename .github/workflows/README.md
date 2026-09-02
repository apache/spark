# Continuous Integration on GitHub Actions

The continuous integration build is critical to the project's health and development velocity. It enables contributors to make changes to Spark with the confidence that they have not inadvertently broken anything.

There are several considerations to keep in mind when making changes to the build.

## Resource usage limits

The Apache Software Foundation (ASF) imposes [resource usage limits][limit] on GitHub Actions, including on the number of concurrent running jobs as well as on the total number of [run minutes used][usage] per week and rolling five-day window. When we hit these limits it can cause jobs to queue up, bringing development to a crawl.

[limit]: https://infra.apache.org/github-actions-policy.html
[usage]: https://github.com/apache/spark/actions/metrics/usage

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
