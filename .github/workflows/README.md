# Continuous Integration on GitHub Actions

The continuous integration build is critical to enabling contributors to make changes to Spark with the confidence that they have not inadvertently broken anything.

There are several considerations to keep in mind when making changes to the build.

## Push heavy workflows out to forks

The Apache Software Foundation (ASF) has an Enterprise GitHub plan. This covers all repos under [the `apache` GitHub organization][org]. There is an [organization-wide limit][limit] on the number of concurrent jobs that can run on GitHub Actions. This limit can often cause jobs to queue up, bringing development to a crawl.

[org]: https://github.com/apache
[limit]: https://docs.github.com/en/actions/reference/limits#job-concurrency-limits-for-github-hosted-runners

Workflows triggered by `on: pull_request` [count against the base repo's (i.e. this repo's) limit][base] on concurrent jobs. For this reason it's [important] to use `on: push` for heavy workflows. `on: push` workflows count against the fork's Actions limit and not ours, enabling more jobs to run concurrently without hitting ASF organization-wide limits.

[base]: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull-request-events-for-forked-repositories
[important]: https://github.com/apache/spark/pull/32092

### Downsides of avoiding `on: pull_request`

Keeping the build running is a top priority, but it comes with some downsides, mainly because several features of GitHub Actions work best (or work only) when using `on: pull_request`.

A key example is that when using `on: pull_request` GitHub creates a stable merge reference that all jobs associated with the pull request use. Without this, we have to be [careful to recreate a stable reference ourselves][ref] so that all jobs are testing the same code.

[ref]: https://github.com/apache/spark/pull/55879

Another example is inline pull request annotations. These do not work naturally with `on: push` as they require the workflow that creates the annotations to run on the base repository (and not a fork) and run against the exact latest commit on the pull request (as opposed to a synthetic merge commit). It's possible to work around these problems, but it's [not pretty].

[not pretty]: https://github.com/apache/spark/pull/57015

If in the future GitHub somehow allows workflows to run `on: pull_request` while counting those jobs against the forks' limits instead of ours, that would help eliminate various distortions of our build caused by the move away from `on: pull_request`. We would get the best functionality from GitHub Actions without hitting job run limits.

## Use composite actions

If you find yourself repeating the same set of instructions across multiple jobs, extract them into a reusable [composite action].

[composite action]: https://docs.github.com/en/actions/tutorials/create-actions/create-a-composite-action

## Security considerations

Keep these [security considerations][security] in mind when authoring workflows, as it's possible for malicious forks to use poorly written workflows to steal secrets or merge unapproved code.

[security]: https://cwiki.apache.org/confluence/spaces/BUILDS/pages/321719166/GitHub+Actions+Security
