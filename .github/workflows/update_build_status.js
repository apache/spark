/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

module.exports = async ({github, context, core, env = process.env, log = console}) => {
  // Open PRs are read with one GraphQL query per page of PRs. For each PR it returns
  // the Build checks on the head commit (statusCheckRollup.commit) and the fork's
  // workflow runs for that commit (headRef.target), both as GitHub Actions check
  // suites: fork runs are Actions runs, and Build checks written with the GITHUB_TOKEN
  // land in Actions suites too. On the PR side, only suites containing Build checks
  // are relevant; unrelated upstream workflow runs must not fill the page.
  //
  // A PR rarely has more than a few such suites. A fork's default branch can collect
  // thousands from scheduled workflows, but suites are listed oldest first and a push
  // creates its runs' suites right away, so they are still among the first. Should a
  // page ever not hold everything a PR needs, the PR fails loudly rather than being
  // synced from partial data.
  //
  // GitHub aborts a GraphQL request after 10 seconds and allows 60 seconds of GraphQL
  // CPU time per minute. A page of 25 PRs takes about 3 seconds, so each page is
  // followed by a pause as long as it took.
  // https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api
  const GITHUB_ACTIONS_APP_ID = 15368
  const PR_PAGE_SIZE = 25
  // Name of the mirrored PR check, not the workflow.
  const BUILD_CHECK_NAME = 'Build'

  // Quick syncs cover only open PRs updated in the last QUICK_SYNC_LOOKBACK_DAYS. A
  // push, comment, review, or label change updates a PR, but a check change does not.
  // Fork runs usually finish within hours of a push, and most reruns happen within
  // days. A later rerun, or a gap in quick syncs longer than the lookback, is picked
  // up by the next full sync, which covers all open PRs. Both schedules are defined
  // in update_build_status.yml.
  const QUICK_SYNC_LOOKBACK_DAYS = 3
  const fullSync = env.FULL_SYNC == 'true'
  const updatedSince = fullSync ? 0
    : Date.now() - QUICK_SYNC_LOOKBACK_DAYS * 24 * 60 * 60 * 1000

  // A run-linked Build check points at the fork's build_main.yml run for the PR branch.
  // Right after a push that run may not be registered yet, so a PR updated in the last
  // NEW_RUN_GRACE_MINUTES without a usable run is left for a later pass instead of
  // receiving action_required guidance.
  const BUILD_WORKFLOW = '.github/workflows/build_main.yml'
  const NEW_RUN_GRACE_MINUTES = 5
  const runGraceSince = Date.now() - NEW_RUN_GRACE_MINUTES * 60 * 1000

  const prPageQuery = `
    query($owner: String!, $repo: String!, $size: Int!, $cursor: String, $appId: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequests(states: OPEN, first: $size, after: $cursor,
            orderBy: {field: UPDATED_AT, direction: DESC}) {
          pageInfo { hasNextPage endCursor }
          nodes {
            number
            updatedAt
            headRefOid
            statusCheckRollup {
              commit {
                oid
                checkSuites(first: 10, filterBy: {
                  appId: $appId, checkName: "${BUILD_CHECK_NAME}"
                }) {
                  pageInfo { hasNextPage }
                  nodes {
                    checkRuns(first: 10,
                        filterBy: {
                          checkName: "${BUILD_CHECK_NAME}", checkType: LATEST
                        }) {
                      pageInfo { hasNextPage }
                      nodes { databaseId status conclusion text }
                    }
                  }
                }
              }
            }
            headRef {
              name
              repository { name owner { login } }
              target {
                oid
                ... on Commit {
                  checkSuites(first: 10, filterBy: {appId: $appId}) {
                    pageInfo { hasNextPage }
                    nodes {
                      status
                      conclusion
                      branch { name }
                      workflowRun { databaseId file { path } }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }`

  // Yield the open PRs updated since updatedSince one page at a time, most recently
  // updated first, pausing after each page as long as it took (see above). A PR
  // updated during the pass moves to the front and may be missed, but it is then
  // recent enough for the next pass.
  async function* listOpenPRs() {
    let cursor = null
    for (;;) {
      const started = Date.now()
      const data = await github.graphql(prPageQuery, {
        ...context.repo,
        size: PR_PAGE_SIZE,
        cursor,
        appId: GITHUB_ACTIONS_APP_ID
      })
      const took = Date.now() - started
      const page = data.repository.pullRequests
      const recent = page.nodes.filter(node =>
        Date.parse(node.updatedAt) >= updatedSince)
      yield recent
      if (recent.length < page.nodes.length || !page.pageInfo.hasNextPage) {
        return
      }
      cursor = page.pageInfo.endCursor
      await new Promise(resolve => setTimeout(resolve, took))
    }
  }

  // Flatten a PR from a page into its number, updatedAt, head sha, fork owner, repo and
  // branch, and:
  // - buildChecks: the head commit's Build checks.
  // - forkRuns: the fork's workflow runs for the head commit, by run ID, and
  //   forkRunsTruncated: whether the page held only some of them.
  // - buildRun: the fork's newest build_main.yml run for the PR branch, if any. The
  //   branch matters because the same commit can have runs from pushes to different
  //   branches, including a skipped master run and a build on the PR branch.
  // Statuses and conclusions are lowercased to match the check-runs REST API.
  // Return null if the fork branch is gone or not at the PR head commit (the branch
  // was just pushed to and the PR has yet to follow). Throw if the page does not hold
  // all of the PR's Build checks.
  const toPR = (node) => {
    const target = node.headRef?.target
    const commit = node.statusCheckRollup?.commit
    if (target?.oid != node.headRefOid || (commit && commit.oid != node.headRefOid)) {
      return null
    }
    // A commit without any checks or statuses has no rollup.
    const suites = commit?.checkSuites.nodes ?? []
    if (commit?.checkSuites.pageInfo.hasNextPage
        || suites.some(s => s.checkRuns.pageInfo.hasNextPage)) {
      throw new Error('The PR head commit has more check suites or Build checks '
        + 'than a page holds')
    }
    // checkName filtering can repeat suites; process each Build check only once.
    const checks = new Map(suites.flatMap(s => s.checkRuns.nodes).map(cr => [cr.databaseId, cr]))
    const lower = (value) => value?.toLowerCase()
    const runs = target.checkSuites.nodes.filter(s => s.workflowRun).map(s => ({
      id: s.workflowRun.databaseId,
      status: lower(s.status),
      conclusion: lower(s.conclusion),
      workflow: s.workflowRun.file?.path,
      branch: s.branch?.name
    }))
    return {
      number: node.number,
      updatedAt: node.updatedAt,
      sha: node.headRefOid,
      forkOwner: node.headRef.repository.owner.login,
      forkRepo: node.headRef.repository.name,
      branch: node.headRef.name,
      buildChecks: [...checks.values()].map(cr => ({
        id: cr.databaseId,
        status: lower(cr.status),
        conclusion: lower(cr.conclusion),
        text: cr.text
      })),
      forkRuns: new Map(runs.map(run => [String(run.id), run])),
      forkRunsTruncated: target.checkSuites.pageInfo.hasNextPage,
      buildRun: runs.findLast(run => run.workflow == BUILD_WORKFLOW
        && run.branch == node.headRef.name)
    }
  }

  // The fork run ID a Build check points at, stored as JSON in its output text, or
  // undefined if the text is not that JSON (e.g. a check from an older version or a
  // manual run).
  const runIdOf = (cr) => {
    try {
      return JSON.parse(cr.text).run_id
    } catch {
      return undefined
    }
  }

  // The Build check state that shows a fork run's state. The check-runs API accepts
  // fewer values than a run reports: the other pending statuses (requested, waiting,
  // pending) become queued, and the other conclusions (startup_failure, stale, or
  // none), which all mean the run produced no test result, become failure.
  const checkState = (run) => {
    if (run.status != 'completed') {
      return {status: run.status == 'in_progress' ? 'in_progress' : 'queued'}
    }
    const conclusions = ['success', 'failure', 'neutral', 'cancelled', 'timed_out',
      'action_required', 'skipped']
    return {
      status: 'completed',
      conclusion: conclusions.includes(run.conclusion) ? run.conclusion : 'failure'
    }
  }
  const label = (state) => state.status == 'completed' ? state.conclusion : state.status
  const sameState = (a, b) => a.status == b.status
    && (a.status != 'completed' || a.conclusion == b.conclusion)

  // Record PR-specific failures and fail the step after syncing the rest.
  // A failed page query instead aborts the pass.
  const failedPRs = new Set()

  // The log lists only the Build checks a pass creates or updates, and errors, each
  // with its PR and head commit. Everything else is counted for a summary at the end.
  const count = {prs: 0, skipped: 0, updated: 0, created: 0, unchanged: 0, notApplied: 0}
  const prUrl = `https://github.com/${context.repo.owner}/${context.repo.repo}/pull/`

  log.log(fullSync ? 'Full sync: all open PRs'
    : 'Quick sync: open PRs updated in the last ' + QUICK_SYNC_LOOKBACK_DAYS + ' days')
  for await (const prs of listOpenPRs()) {
    for (const node of prs) {
      count.prs++
      const ref = prUrl + node.number + ' ' + node.headRefOid.slice(0, 7)
      let pr
      try {
        pr = toPR(node)
      } catch (error) {
        log.error(ref + ': ' + error.message + '; raise the limits in prPageQuery')
        failedPRs.add(node.number)
        continue
      }
      if (!pr) {
        count.skipped++
        continue
      }

      // Sync every Build check that points at a fork run, not just the first: a commit
      // can carry more than one (e.g. left by the notify workflow this one replaced).
      // An action_required guidance check is not linked to a run.
      let syncableBuildCheck = false
      for (const cr of pr.buildChecks) {
        if (cr.conclusion == 'action_required') {
          continue
        }
        const runId = runIdOf(cr)
        const run = pr.forkRuns.get(String(runId))
        if (!run) {
          if (runId && pr.forkRunsTruncated) {
            // The run may be among the fork's check suites the page left out. Keep
            // the backfill from replacing the check, and fail the PR to get noticed.
            log.error(ref + ': fork run ' + runId + ' not in the first fork check '
              + 'suites; raise the limits in prPageQuery')
            failedPRs.add(pr.number)
            syncableBuildCheck = true
          }
          // Otherwise the check's output is unusable or its fork run was deleted, so
          // it can never be synced; the backfill below replaces it.
          continue
        }
        syncableBuildCheck = true

        // Leave a check alone if it already shows the run's state, as nearly all do.
        const state = checkState(run)
        if (sameState(cr, state)) {
          count.unchanged++
          continue
        }
        const change = label(cr) + ' -> ' + label(state)
        try {
          const {data: updated} = await github.rest.checks.update({
            ...context.repo,
            check_run_id: cr.id,
            ...state
          })
          // A successful request can leave the state unchanged, notably when trying
          // to reopen a completed check after a rerun.
          if (sameState(cr, updated)) {
            count.unchanged++
          } else {
            log.log(ref + ': ' + label(cr) + ' -> ' + label(updated))
            count.updated++
          }
          if (!sameState(updated, state)) {
            count.notApplied++
          }
        } catch (error) {
          log.error(ref + ': ' + change + ' failed: ' + error.message)
          failedPRs.add(pr.number)
        }
      }

      // No syncable Build check: link one to the PR branch's build_main.yml run, or
      // create action_required guidance if no run exists, it was skipped, or the PR
      // is from fork master. Fork master is reserved for syncing upstream; new PR
      // builds must use other branches, even if an older workflow ran on master.
      // Existing run-linked checks are still synchronized above.
      if (!syncableBuildCheck) {
        const run = pr.branch == 'master' || pr.buildRun?.conclusion == 'skipped'
          ? undefined : pr.buildRun
        if (!run) {
          // Already provided guidance, or still within the run-registration grace period.
          if (pr.buildChecks.some(cr => cr.conclusion == 'action_required')
              || Date.parse(pr.updatedAt) > runGraceSince) {
            count.skipped++
            continue
          }
        }

        const state = run ? checkState(run)
          : {status: 'completed', conclusion: 'action_required'}
        const check = {
          ...context.repo, name: BUILD_CHECK_NAME, head_sha: pr.sha, ...state
        }
        const change = 'none -> ' + label(state)
        try {
          if (run) {
            const actions_url = 'https://github.com/' + pr.forkOwner + '/' + pr.forkRepo
              + '/actions/runs/' + run.id
            check.output = {
              title: 'Test results',
              summary: '[See test results](' + actions_url + ')\n\n'
                + 'If the tests fail for reasons unrelated to this pull request, '
                + 'please rerun the workflow in your forked repository.\n'
                + 'If the failures are related to this pull request, '
                + 'please investigate them and push follow-up changes.',
              text: JSON.stringify({
                owner: pr.forkOwner,
                repo: pr.forkRepo,
                run_id: run.id
              })
            }
            check.details_url = actions_url
          } else {
            // No usable run, or a fork-master PR: explain the required setup.
            check.output = {
              title: 'PR build setup requires attention',
              summary: `
  A supported Build check is not available for this PR head commit.

  1. If you did not enable GitHub Actions in your forked repository, please enable it by clicking the button as shown in the image below. See also [Managing GitHub Actions settings for a repository](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository) for more details.
  2. For PR build status, use a branch other than \`master\` in your fork.`
                + (pr.branch == 'master' ? '' : `

  After fixing the cause, push a new commit to your PR branch to start the build.`),
              images: [
                {
                  alt: 'enabling workflows button',
                  image_url: 'https://raw.githubusercontent.com/apache/spark/master/.github/workflows/images/workflow-enable-button.png'
                }
              ]
            }
          }
          await github.rest.checks.create(check)
          log.log(ref + ': ' + change)
          count.created++
        } catch (error) {
          log.error(ref + ': ' + change + ' failed: ' + error.message)
          failedPRs.add(pr.number)
        }
      }
    }
  }

  log.log(count.prs + ' PRs checked, ' + count.skipped + ' skipped. '
    + 'Build checks: ' + count.updated + ' updated, ' + count.created + ' created, '
    + count.unchanged + ' unchanged.')

  if (count.notApplied > 0) {
    core.warning(count.notApplied + ' check update(s) did not reach the requested '
      + 'state. GitHub cannot reopen completed checks.')
  }

  if (failedPRs.size > 0) {
    core.setFailed('Failed to sync ' + failedPRs.size + ' PR(s), will retry next pass: #'
      + [...failedPRs].join(', #'))
  }
};
