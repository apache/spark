# Pipelined Shuffle Dependency & Concurrent Stage Scheduling

A spec for running data-dependent stages of a single job concurrently, connected by a shuffle the
consumer reads incrementally.

---

## 1. Motivation

Today, when two stages of a job are connected by a shuffle, they run one after another: the consumer
stage does not start until the producer's shuffle output is fully materialized. (Independent stages —
ones not connected by a shuffle — already run concurrently.) Some workloads need even
shuffle-connected stages to run **concurrently**, with the consumer reading the producer's output
**as it is produced** rather than after the producer finishes. This spec introduces the scheduler
primitives to express and run that.

"Run these stages concurrently" and "the connecting shuffle is incremental" are the same decision
seen from two sides: co-scheduling a producer and consumer is only useful if the edge is readable
before the producer completes.

---

## 2. Primitives

### 2.1 Pipelined shuffle dependency (PSD)

A shuffle dependency declared **incrementally readable**: a consumer stage may begin reading its
output while the producer stage is still running.

- It is a shuffle dependency (has a `shuffleId`, partitioner, map/reduce sides); the *pipelined*
  property is a binding part of the scheduler contract, not an advisory hint. ("Pipelined",
  "incremental", and "transient" all describe this same edge in this doc — read as its output being
  streamed to the consumer as produced, not stored.)
- The property is set during **physical planning** (an execution concern, not a logical-plan one)
  and carried into the `ShuffleDependency` the `DAGScheduler` reads at stage-creation time.
- It is also the **per-dependency selector** for the shuffle implementation: the shuffle layer maps a
  pipelined dependency to an incremental `ShuffleManager` and everything else to the default, so one
  job with both regular and pipelined groups uses the right implementation for each. The scheduler
  construct stays generic; the shuffle implementation stays pluggable.
  - For example, an additional conf (e.g. `spark.shuffle.manager.incremental`) can be introduced to
    specify the incremental shuffle implementation used for pipelined shuffle dependencies, alongside
    the existing `spark.shuffle.manager` for the default.

**What the subtype inherits vs. what the scheduler skips.** A pipelined shuffle dependency is modeled
as a `ShuffleDependency` subtype, which cleanly divides `ShuffleDependency`'s behavior in two — an
invariant every component that handles the type must preserve:
- *Inherited — it is treated as a real shuffle dependency:* (1) **stage-boundary splitting** —
  `getShuffleDependenciesAndResourceProfiles` matches `case shuffleDep: ShuffleDependency`
  (`DAGScheduler.scala:882`), so the subtype splits producer and consumer into separate stages
  automatically; this is what §3 ("introduces a shuffle boundary exactly as a regular one does")
  relies on. (2) **transport** — `shuffleId`, `ShuffleWriter`/`ShuffleReader`, and `ShuffleManager`
  registration, all from the base constructor.
- *Scheduler-skipped — it must not be treated as a materialized shuffle for the post-boundary
  lifecycle.* The one clean per-dependency opt-out is `MapOutputTracker` registration: it is
  `DAGScheduler`-driven at `createShuffleMapStage`, gated by `!containsShuffle`
  (`DAGScheduler.scala:667-673`), so a pipelined dependency is simply never registered — clean rather
  than "inherit then disable". Two further behaviors follow from that skip rather than being gated
  separately: executor-loss output-removal-then-resubmit never acts on a pipelined shuffle because
  nothing was registered to remove (§6); and cross-job stage *reuse* is prevented not by withholding
  the stage from `shuffleIdToMapStage` — the producer is enrolled there (`DAGScheduler.scala:664`)
  like any shuffle, since the consumer and the failure paths resolve through it — but by rejecting a
  producer stage that would accrue a second jobId (§4).
- *Match-site invariant:* a marker subtype relies on every existing `case _: ShuffleDependency` site
  meaning "materialized" for the new type. The reuse/tracking/loss sites are safe because they are
  scheduler-gated as above; the remaining sites were audited and treat a pipelined dependency as an
  ordinary materialized shuffle wherever the subtype does not specifically diverge. Preserving this
  is an invariant for implementers, not something to rediscover.

A **regular shuffle dependency (RSD)** is an ordinary shuffle dependency: its output must be fully
materialized before any consumer reads it.

Note: the name *pipelined* is deliberately chosen over *streaming*. The property is that a consumer
reads producer output as it is produced — software-pipelining of dependent stages — a general
execution capability. Streaming / real-time mode (RTM) is the first caller, but nothing about the
primitive is streaming-specific.

### 2.2 Pipelined group (PG)

The set of stages connected to one another through pipelined edges — the connected component of the
stage DAG when only pipelined edges are considered.

- A pipelined group has **two or more members by construction** — it is a connected component over
  pipelined edges, and an edge has two endpoints. A stage with no incident pipelined edge is simply an
  **ordinary stage, a member of no group**; it keeps today's per-stage behavior because it is a
  non-member, not because a group-of-one exempts it.
- The group — not the edge or the individual stage — is the unit of **admission**, **slot
  checking**, **completion**, and **failure**.

**External input of PG:** a regular shuffle dependency whose consumer is in the group and whose
producer is not — i.e. a normal materialized parent of the group.

**Outputs of PG:** a `ResultStage` **may** be a member of a pipelined group — a pipelined consumer
that produces the job's result (its commit handling is §5) — but a PG **need not** contain one.
When it does not, its outputs to the rest of the DAG are regular (materialized) shuffle edges from
its members to consumers outside the group, and there may be **more than one** — e.g. two members
each feeding a distinct downstream stage. Such a PG is an interior component whose materialized
outputs feed downstream groups (§7); in that respect it behaves like a barrier stage embedded in a
larger DAG — an interior unit with regular-shuffle edges in and out.

**Example.** Take a job with stage DAG `Z -> A -> B -> C`, where `Z -> A` and `B -> C` are regular
edges and `A -> B` is pipelined. Grouping over pipelined edges gives one group
`PG = {A, B}`; `Z` and `C` are ordinary (non-member) stages. `Z`'s output is the group's external input, which the
group waits for; `A` and `B` run concurrently, `B` reading `A`'s output as `A` produces it; and
`B`'s output to `C` is a materialized output of the group that `C` consumes by normal
materialize-before-read sequencing.

---

## 3. Group formation

- **Stage decomposition is unchanged.** A pipelined dependency introduces a shuffle boundary exactly
  as a regular one does; the set of stages and their partitioning are identical. The pipelined
  property changes only *when* stages run relative to one another, never *how the plan is cut into
  stages*.
- **Group = connected component over pipelined edges.** Two stages joined by a pipelined edge are in
  the same group; the group is the transitive closure over pipelined edges.
- **Membership is derived, not stored.** Group membership is a function of the pipelined-edge
  structure of the stage graph, recomputed on demand wherever a scheduling decision needs it
  (admission, completion, failure) rather than stored in a group object at stage-creation time. It is
  stable because that graph does not change after a stage is created. A stage with no incident
  pipelined edge is a member of no group and follows the existing per-stage path.
  - *Derived extent, but not zero state.* Deriving group extent from the graph means there is no group
    object to populate at formation or tear down on abort. It does not mean the feature is entirely
    stateless: the deferred-completion buffer (a member that finished ahead of its group, §5) is
    per-stage state that still must be discarded on group failure / job cancellation. Deriving extent
    narrows the state that can go stale to that buffer; it does not eliminate it.
- **A DAG may contain multiple pipelined groups.** Disjoint sets of pipelined edges form distinct
  groups, and a group's materialized output may feed another group (§7). Independent groups (no
  dependency path between them) may be admitted and run concurrently, subject to the cross-group
  slot arbitration in §4.1; a group that consumes another group's materialized output waits for it,
  by the normal materialize-before-read sequencing on that regular edge. This mirrors how a DAG may
  contain multiple barrier stages.

---

## 4. Scheduling & admission

- **A pipelined edge is non-sequencing.** The consumer of a pipelined dependency does not wait for
  its producer to materialize. (A regular-dependency consumer still waits — the default behavior.)
- **Group readiness.** A group is ready to be admitted when every external input (its regular
  materialized parents) is available — the same precondition a normal stage has today, lifted to the
  group. Pipelined parents inside the group impose no readiness precondition.
- **Gang admission (all-or-nothing).** A pipelined group is admitted only if the cluster can currently
  run all tasks of all member stages **concurrently**; admission then submits every member stage at
  once. A pipelined group is never left with some members running while others wait on slots the
  running members occupy.
  - *A non-member stage is unaffected:* it is admitted exactly as a normal stage is today — submitted
    when its parents are available, filling slots incrementally, with no all-at-once requirement. Gang
    admission applies only to a group (two or more stages connected by pipelined edges).
- **Slot check.** The group's aggregate concurrent-task demand — the sum of `numTasks` over member
  stages — is compared against the number of **currently-free** slots: the cluster's total
  concurrent-task capacity minus the demand already committed to it — tasks already running **and
  tasks already submitted but not yet launched**. If the group needs more than are free, admission
  fails fast, since the group could never become fully co-resident. (Free rather than total is
  essential once more than one group can be admitted; §4.1 explains why.)
  - *Why submitted-but-not-launched demand counts.* Admitting a group submits its member stages,
    which places their tasks in the task scheduler's queue at once — before they launch. Counting only
    running tasks would miss a group admitted an instant earlier (its tasks queued, not yet running)
    and admit a second group that cannot co-fit with it; counting committed demand — running plus
    queued — closes that. The queued tasks are themselves the record of committed demand, so no
    separate reservation is tracked.
  - *How capacity is measured:* the total concurrent-task capacity is the value barrier's slot check
    uses (`sc.maxNumConcurrentTasks` — for the group's resource profile, the number of task slots
    across active executors, i.e. cores divided by cores-per-task), not `spark.default.parallelism`
    (a default partition count, unrelated to how many tasks can run concurrently).
- **Single resource profile per group (v1).** A resource profile is the executor/task resource
  requirement (cores, memory, GPUs, ...) a stage runs under; the number of concurrent slots is
  defined *per profile* (a cluster may run many concurrent CPU tasks but few GPU tasks). Comparing
  one demand against one capacity is therefore only well-defined when all member stages of a group
  share a single profile. v1 requires that and rejects a mixed-profile group (fail-fast, §9);
  per-profile accounting — checking each profile's demand against that profile's own capacity — is a
  follow-up, not needed for the streaming shapes whose members share the default profile.
- **Co-residency.** Once admitted, all member stages of a group are simultaneously running.
- **Single ownership and 1:1 (v1).** In v1 a pipelined producer feeds exactly one consumer and
  belongs to exactly one job. These are two separate restrictions. They follow from what a pipelined
  shuffle *is* (a transient edge), not from how any particular caller builds its DAG — the pipelined
  dependency and PG are generic `DAGScheduler` constructs that code can depend on directly, not only
  through SQL / real-time mode:
  - *No cross-job / cross-time reuse — intrinsic.* A pipelined shuffle is transient: a once-through
    live stream with no retained, addressable output. So, unlike a regular shuffle, there is nothing
    for a second job to reuse — reuse across jobs is unsound for it.
    - This must be enforced, not assumed: from the scheduler's perspective a shuffle-map stage *can*
      be reused unless something forbids it. A second job over the same pipelined dependency is an
      **error, rejected fail-fast** — never a silent reuse and never a silent recompute. Spark would
      otherwise reuse a shuffle in two ways, and **both** are blocked. (1) Stage-object reuse via
      `shuffleIdToMapStage`: the producer stage is enrolled there like any shuffle-map stage (it must
      be — the consumer binds to its producer through this map, and §6's `FetchFailed` recovery
      resolves the producer via `shuffleIdToMapStage(shuffleId)`, a lookup that throws if the entry is
      absent). So a second job finds the existing stage rather than getting a fresh one, and the stage
      would accrue that job's id (`updateJobIdStageIdMaps`); a pipelined producer stage that would
      carry more than one jobId is **rejected at that point** (fail-fast, §9). This is a new check —
      the base scheduler shares stages across jobs freely — and it is the reachable enforcement of
      run-once, not a defensive invariant. (2) Output-availability reuse: a re-created producer would
      be skipped as already-done because its outputs are still registered in `MapOutputTracker`
      (`getMissingParentStages` treats a registered stage as available). The fix for (2) is the same
      one §6 requires for executor loss: a pipelined shuffle is not tracked in `MapOutputTracker` at
      all — its availability is owned elsewhere — so executor-loss removal cannot act on it either.
    - (A producer may separately write a durable *materialized* output edge — a distinct regular
      `ShuffleDependency` that reuses normally. Run-once binds only the transient edge.)
  - *1:1 within the group (v1) — deferred, not intrinsic.* v1 rejects a pipelined producer with more
    than one consuming stage, but 1:N fan-out is a supported model that v1 defers rather than an
    incompatibility (§9): co-scheduled consumers would read the live stream via multicast, and
    non-co-scheduled consumers read a materialized edge the producer also writes.

### 4.1 Cross-group admission (multiple groups / groups vs. regular jobs)

A pipelined group holds its slots for its whole run, so admission is decided against currently-free
slots.

- **Why free slots, not total** (the slot check itself is defined in §4). Free capacity subtracts the
  committed demand — tasks running *and* tasks already queued — for every group and regular job, so a
  group is admitted only if its full demand fits in what the others leave free. This is what makes
  group-vs-group co-residency safe: comparing against *total* capacity would let two groups each pass
  yet fail to co-fit, which is exactly the partial co-residency that gang admission forbids. Counting
  queued (not just running) demand is what closes the gap between the two groups, since an
  admitted group's tasks are queued before they run. This is where the check diverges from barrier,
  which compares demand against total capacity; computing free capacity this way is a new computation,
  not barrier's check reused verbatim.
- **No waiting queue.** A group that doesn't fit fails its admission; it never sits in a queue holding
  slots incrementally. This keeps the scheduler change minimal: no group ever *sits* partially
  resident holding slots a sibling is blocked on. Against another group this is deadlock-free by
  construction — counting committed demand never over-admits two groups that cannot co-fit.
- **Two failure reasons, one path.** Demand > total capacity can never fit (a plan/sizing error);
  demand > free slots but ≤ total could fit later. Both fail admission.
- **Retry (v1: caller's decision; scheduler-side retry is a post-v1 refinement).** In v1 the
  scheduler does not retry a failed admission — it fails the job, and retry is the caller's decision
  (for a streaming query, the batch-execution loop restarting the batch with its own backoff). This
  is adequate only for a simple `prefix* -> PG -> suffix*` shape run by a caller that has its own
  restart loop. Its unit is the whole job, so it has two weaknesses: with concurrent work (sibling
  stages, other PGs) or an already-committed prefix, restarting the job discards work it cannot
  selectively preserve; and a directly-depended-on PG may have no retrying caller at all. The result
  is that a transient slot shortfall either kills the job or forces over-provisioning.
  - *(Refinement, post-v1.)* The scheduler retries **admission of the PG specifically** — hold the
    group, re-run the gang slot-check on a timer, admit when it fits, fail after a bounded number of
    attempts — leaving any completed prefix and concurrent stages untouched. This mirrors barrier's
    transient-shortfall retry (re-post on a scheduled interval bounded by a max-failure count, then
    fail; `DAGScheduler` `BarrierJobSlotsNumberCheckFailed` path), the difference being that a
    mid-DAG PG retries its own admission rather than re-posting the whole job (barrier can re-post
    the job only because its check runs before any stage executes). Making transient-shortfall
    tolerance a property of the construct, rather than a burden each caller re-implements, is why it
    belongs in the scheduler.

---

## 5. Completion

- **Group-observable completion.** A member stage is not observably finished until **all** member
  stages of its group have completed successfully. A member that finishes ahead of the others cannot
  advance stage or job completion, nor make its output visible to a downstream consumer, until the
  group does — whether that output is a `ResultStage`'s job result or a materialized shuffle feeding a
  downstream group (§2.2).
- **Defer the finish decision, not per-task work.** When a member finishes before its group, only its
  stage-finish / job-finish transition is deferred until group completion. Per-task side effects that
  must always run — accumulator updates, output-commit coordination, task-end listener events — run
  immediately.
  - *Deferred output registration.* A member's materialized output edge (to a consumer outside the
    group, §7) is written as its tasks run, but its map-output registration is **deferred to group
    completion** — so an out-of-group consumer, which waits for the group anyway (§7), only ever sees
    the single committed version. A failed intermediate attempt's blocks are simply orphaned (never
    registered), exactly as for a resubmitted regular map stage today. This is what makes a producer
    that writes both an incremental (in-group) and a materialized (out-of-group) output edge
    consistency-clean: the group's internal replay never reaches the materialized consumer, because
    it has not started.
- **Replay window.** There is a window between a member finishing and group completion. A failure in
  that window is a group failure: the deferred finish transitions are dropped, and recovery is as in
  §6 — the group reruns as a unit (in v1, via caller rerun).
- **In-group result-stage side effects must be idempotent.** Per-task side effects run immediately
  (above), including a result stage's output commit. If a result task commits and a sibling then
  fails in the replay window, the group reruns and re-delivers that output (in v1, via caller rerun) —
  the standard streaming model, where a batch is re-delivered on recovery and the sink must absorb it.
  So v1 requires an in-group result stage's side effects to be idempotent — the sink must absorb
  re-delivery. This is a semantic contract on the caller, not a group-creation check: unlike the
  idioms in §9, whether a sink tolerates re-delivery is a runtime property the scheduler cannot
  inspect at stage/group creation.
- **Group rerun and per-partition commit authorization (v1 gets this for free; a scheduler-side rerun
  must handle it).** A PG is an atomic scheduling unit: there are no per-task or per-partition replays
  within it. `OutputCommitCoordinator` is built on a narrower assumption — it authorizes exactly one
  attempt per partition and permanently denies any later request for a partition that already
  committed. That is sound today because a committed partition is never recomputed: a stage rerun
  (e.g. on fetch failure) recomputes only its *missing* partitions, so a committed task never needs to
  commit again, and the permanent deny is correct. A group rerun breaks that assumption — rerunning
  the group reruns a result stage whose tasks already succeeded and committed, and those committed
  partitions must be allowed to commit again. Whether that is a problem depends on *how* the rerun
  happens:
  - *In v1 it is not a problem.* v1 has no scheduler-side in-job group rerun; recovery fails the job
    and the caller reruns it (§6). A caller rerun is a **new job with fresh stage objects and fresh
    (globally monotonic) stage ids**, so it lands on a fresh coordinator `StageState` with no prior
    committers — the committed-partition re-commit just works. v1 needs only ordinary `stageEnd`
    cleanup on group teardown, no special reset.
  - *A post-v1 scheduler-side group rerun must reset it explicitly.* If a later version reruns a group
    *in place* (reusing stage ids, bumping the attempt) instead of via caller rerun, the coordinator
    still holds the previous attempt's authorized committers and would deny the re-commit (a `Success`
    clears nothing; only a *failed* holder's slot is cleared today, and `stageStart` on the same id
    explicitly reuses the prior attempt's committers). Such a rerun must therefore either run members
    under **fresh stage ids** (a fresh `StageState`) or **reset** the committed state for those stages
    in the `OutputCommitCoordinator` (e.g. `stageEnd` before the rerun). This is the contract that
    path owes; it is not needed until that path exists.

### 5.1 Observable completion events (listener bus)

The listener bus is an external contract that monitoring tools depend on, so it is worth stating
exactly when each event is delivered. The rule follows directly from group-atomic completion (the
point at which the group's outputs become observable — distinct from the per-task output-commit
above): **task-level events flow in real time, but stage-completion and job-completion events are
held until the group completes — so a listener never observes a member as *successfully completed*
before the group as a whole has.**

| Event | Timing | Rationale |
|-------|--------|-----------|
| `SparkListenerTaskStart` / `SparkListenerTaskEnd` | Real time, as they occur | Per-task facts are true when they happen; a group's members genuinely run concurrently. Deferring these would freeze a member's live progress and metrics for the whole group's duration. Note a successful `TaskEnd` means "this task finished," not "its output is committed" — already true in Spark, since a stage attempt can later be discarded. |
| `SparkListenerStageSubmitted` | Real time, at group admission | All member stages are submitted together (§4); a monitor should show them active simultaneously. |
| `SparkListenerStageCompleted` | Deferred to group completion; on group failure, emitted with a failure reason | "Completed" should track group completion, which is atomic at the group level. A member whose tasks finish early is reported as still running until the group completes — which matches the truth that its results are not usable until then. This avoids emitting a success-shaped completion for an attempt that a later group failure would discard. |
| `SparkListenerJobEnd(JobSucceeded)` | At group completion only | Job completion delivers results to the caller and cancels sibling stages; emitting it before the group completes risks double/inconsistent result delivery if the group later fails. Non-negotiable. |
| `SparkListenerJobEnd(JobFailed)` | On group failure | Group-atomic failure (§6): buffered success transitions are dropped, never replayed as success. |

Failure-path consequence: already-emitted task events stand as-is (they were true), consistent with
how Spark treats task events from a stage attempt that is later discarded.

---

## 6. Failure

- **Failure is group-atomic.** Any member task failure, for any reason, fails the whole group.
  - *Task-attempt retry is disabled for group members — first failure escalates.* "Any member task
    failure fails the group" is a `DAGScheduler`-level statement, but there is a retry layer below it:
    within a stage attempt, `TaskSetManager` retries a failed task up to `spark.task.maxFailures`
    (default 4) before the `DAGScheduler` sees the failure at all. For a pipelined producer that
    in-stage retry is not survivable: the first attempt has already emitted part of its output into
    the live incremental stream, so a second attempt re-emits from the start and the consumer sees
    duplicates — a hazard that never reaches the group-failure logic. (This is the case of a failure
    that counts toward task failures, i.e. a plain task exception. A `FetchFailed` does not retry in
    the `TaskSetManager` — it routes to the `DAGScheduler` directly — and is already handled by group
    failure.) So for member stages, task-level retry is disabled: the first attempt failure routes
    straight to group failure (effectively `maxTaskFailures = 1`). This mirrors barrier stages, which
    already escalate a member task failure to whole-stage failure on the first failure rather than
    retrying in place. Consequence: any single flaky member task reruns the whole group (its whole
    batch), consistent with the streaming re-delivery model and the idempotent-sink requirement (§5).
    This is also the mechanism behind §5's "no per-task or per-partition replays within a group":
    first-failure escalation is what makes that true.
  - *Single-stage resubmit is not taken for group members.* The isolated-resubmit branch is never
    taken for a member of a pipelined group: the transient pipelined shuffle cannot be re-read and
    members are co-scheduled, so a lone-stage resubmit is never valid — every member failure instead
    routes to group failure.
    - *Note this differs from the base scheduler,* which handles a task failure by resubmitting just
      that one stage — the `FetchFailed` -> resubmit path, and retry paths generally — recomputing
      the failed stage in isolation and resuming.
  - *A pipelined shuffle's availability is not tracked in `MapOutputTracker`.* This is the mechanism
    that makes "no single-stage resubmit" robust, and it also closes an executor-loss resubmit path.
    A regular shuffle records its map outputs in `MapOutputTracker`, and `ShuffleMapStage.isAvailable`
    is derived from them (`getNumAvailableOutputs`); on executor or host loss the scheduler strips
    those outputs (`removeOutputsOnExecutor` / `removeOutputsOnHost`), which flips `isAvailable` to
    false and resubmits the producer. For a transient pipelined shuffle, resubmitting the producer is
    both wrong (its output was never durably stored, so there is nothing to recompute into) and
    dangerous (the resubmitted producer has no live consumers left to serve and can hang — for
    example, an incremental-shuffle writer that blocks waiting for end-of-stream acknowledgements from
    reducers that have already finished). So a pipelined shuffle is *not* registered with
    `MapOutputTracker`; its map-stage availability is
    tracked separately — on the `ShuffleMapStage` itself (a monotonic completed-partition set) or a
    streaming-specific tracker (`StreamingShuffleOutputTracker`) — and `MapOutputTracker` stays
    streaming-agnostic. Executor/host-loss removal then never touches a pipelined shuffle and never
    flips its availability, so the producer is never resubmitted from that path; genuine losses that
    happen while the group is still running are handled by group-atomic teardown above. This is also
    what makes the "no single-stage resubmit" rule above robust: with no tracked availability to flip,
    the availability-driven resubmit cannot fire in the first place. (Same
    `MapOutputTracker`-must-not-govern-a-transient-shuffle point as the reuse layer in §4, seen from
    the executor-loss side rather than the cross-job-reuse side.)
  - *Teardown is by group membership, not producer availability.* At the end of a batch the producer
    finishes and registers all its map outputs — becoming "available" — while the consumer is still
    draining the remainder, so it is normal, not rare, for the producer to be available while the
    consumer is still running. For a transient pipelined shuffle, "available" does not mean the
    consumer is safe: the data is not durably stored and the consumer is still actively reading it, so
    a producer failure in that window still strands the consumer. Failure propagation that keys off
    producer availability would miss the consumer here; teardown is therefore keyed on group
    membership, so a producer failure always tears down its co-scheduled consumers regardless of the
    producer's availability at the failure instant.
  - *(Refinement, post-v1.)* v1 fails the group on any member's executor loss — safe and hang-free,
    but over-eager: a producer that finished and whose output was already fully consumed can lose its
    executor with no impact on the DAG, yet still triggers a rerun. A more precise, consumer-driven
    signal is a post-v1 optimization: have the streaming reader raise a `FetchFailed` only when it
    actually fails to read its input, so a post-consumption producer loss yields no failure and no
    teardown. This reuses Spark's existing `FetchFailed` channel as the notification, with one
    adaptation — on a pipelined edge it must route to **group failure**, not the base single-stage
    mapper-resubmit — so detection is consumer-driven while recovery stays group-atomic.
- **Recovery.** Group failure tears down all member stages atomically and discards their deferred
  completions. Because the shuffle is transient it cannot be partially recovered; the job fails and
  the caller re-runs it. The group is likewise the recovery unit for a fetch failure on a member's
  *materialized* output edge that arrives after the group has already completed (§7): that too reruns
  the producing group (in v1, via caller rerun), rather than resubmitting the member in isolation.

---

## 7. Interaction with regular dependencies

- **Regular shuffle into a group — supported.** A regular dependency from outside the group to a
  member is a normal materialized parent (an external input); the group waits for it.
  - A lost external durable input reruns the group rather than being recomputed in isolation.
- **Regular shuffle out of a group — supported.** A member may have a regular output edge to a stage
  outside the group; the downstream consumer waits for the group to complete and reads it by normal
  materialize-before-read sequencing. As in §2.2, a group may have more than one such output. These
  external input and output edges are ordinary regular shuffles and behave exactly as today,
  including optimizations like push-based shuffle merge; the pipelined restrictions apply only to the
  group's internal (incremental) edges.
  - *Losing this output after the group completes reruns the group, not the member in isolation.*
    Once the group completes, its deferred map outputs are registered, so this edge is a normal
    registered shuffle — and if the executor holding those blocks is then lost while a downstream
    consumer is reading, the consumer raises a `FetchFailed`. The base scheduler would resolve that to
    the producing member stage (via `shuffleIdToMapStage`) and resubmit *that member* in isolation —
    which is unsatisfiable, since the member's own input was the transient pipelined edge, now gone
    (the missing-parent walk would then chase parents tracked nowhere). The recovery unit is instead
    the whole group: rerunning the group recomputes it from its durable external inputs (the regular
    parents above), regenerating the output for the consumer to re-read — the direct analogue of a
    regular single-stage resubmit, with the group as the unit. This is a recoverable rerun, not an
    unrecoverable one. How the group rerun is driven follows §6: v1 has no scheduler-side group rerun,
    so it is achieved by the caller rerunning the job; a post-v1 scheduler-side group rerun would
    recover it without failing the job. The only new thing is that the trigger can arrive *after* the
    group has completed.
- **Regular shuffle internal to a group — should not occur; checked as an invariant.** Grouping is
  the connected component over *pipelined* edges only (§3), so a regular shuffle is a group
  *boundary*: its producer and consumer fall into different groups by construction, splitting a
  pipelined region rather than sitting inside one. A regular shuffle therefore should never be
  internal to a group. The scheduler still verifies this invariant at stage/group creation and fails
  fast if violated. A regular edge whose endpoints landed in the same group would be a contradiction:
  group membership says co-schedule the two stages, while a regular edge says the consumer must wait
  for the producer to fully materialize first. Such an edge signals inconsistent pipelined marking,
  not a valid plan.
- Pipelined edges are intra-group by construction, so they never cross a group boundary.

---

## 8. Activation

- **Activation is by group membership.** Every behavioral change — admission, completion, and
  failure handling — keys off a single question: is this stage a member of a pipelined group? There
  is no separate enable flag and no half-activated state.
- **Opt-in is expressed by the presence of a pipelined dependency**, which a physical-planning rule
  sets on the relevant exchanges. The scheduler primitive is generic; any feature can write such a
  rule to opt a job into concurrent stage scheduling over an incremental shuffle.

---

## 9. Fail-fast on unsupported idioms

A pipelined group that involves any of the following is rejected at stage/group creation with a clear
error, rather than silently mis-scheduled. Each is a documented limitation of the first version.
The scope throughout is **within a PG** — how each idiom interacts with the members and internal
(pipelined) edges of a group. A group's regular input/output edges are ordinary shuffles and are
unaffected (§7); the entries below do not restrict them.

The rejected idioms fall into two kinds. Some are **moot** under the group failure model (§6):
Spark's stage-level recompute/rollback mechanisms never fire inside a group, because any failure
aborts the whole group and the caller reruns from scratch — so a mechanism whose only job is to
recompute or roll back a stage after a partial failure is never reached. We reject these rather than
carry dead, self-contradictory machinery. Most of the rest are **incompatible** with concurrent
execution itself and would corrupt or hang a group that never fails. One row is neither — an
**invariant** that should hold by construction (§3) but is still checked defensively, and would
signal an inconsistent plan if violated. One further row is **deferred** — a capability that is
well-defined and compatible with the model but not built in v1, so v1 rejects it fail-fast rather
than mis-scheduling it; it is expected to be supported later.

| Rejected condition | Kind | Why rejected |
|--------------------|------|--------------|
| Statically-indeterminate producer | moot | Its recovery is stage rollback-and-recompute, which a group never performs (§6: any failure aborts the group), so the mechanism is never reached. (A producer whose output RDD is classified `INDETERMINATE` — a rerun can yield different data, not just a different order — determinable from the RDD graph at stage-creation time.) |
| Checksum-mismatch full retry | moot | The runtime counterpart to static indeterminism: it checksums each map task's output and, on a cross-attempt mismatch, rolls back and re-runs the succeeding stages. A group never keeps succeeding stages across a retry (§6), so this never fires. |
| Barrier execution in a member stage | incompatible | Barrier mode runs a stage's tasks as an atomic gang with whole-stage retry — on any failure the entire stage is recomputed — which discards and regenerates output a pipelined consumer has already read incrementally (the same hazard as speculative execution below). It also lets tasks rendezvous in lockstep mid-stage (`barrier()`), at odds with per-partition incremental production/consumption on the pipelined edge. |
| Dynamic resource allocation | incompatible | Gang admission needs a stable slot set; reclaiming executors from a pinned-open group can deadlock it. |
| Speculative execution | incompatible | A speculative producer copy races a consumer already reading partial output; no commit barrier protects the read. |
| Push-based shuffle merge as a pipelined (incremental) shuffle | incompatible | Push-based merge exposes output only after a post-completion "finalize" step — the opposite of incremental reads — so it cannot serve as the incremental shuffle within a PG. (A PG's regular input/output edges may still use push-based merge, like any regular shuffle — §7.) |
| Pipelined producer shared across jobs (cross-time reuse) | incompatible | A transient incremental edge is a live, once-through stream with no retained, addressable output, so a consumer running at a different time has nothing to fetch — cross-time / cross-job reuse is intrinsically undefined for it. Enforced by rejecting a pipelined producer stage that would carry more than one jobId (§4): the producer is enrolled in `shuffleIdToMapStage` like any shuffle, so a second job over the same dependency finds that stage and would add its jobId — which is the fail-fast point. (This is a real, reachable check, not a should-never-arise invariant like the regular-shuffle-internal row below.) |
| Pipelined producer with more than one co-scheduled consumer (fan-out / branching) | deferred | 1:N (one producer, many consumers) is a supported model, just not built in v1. How each consumer reads depends on whether it is in the group: an in-group consumer would read the live stream, which requires the producer to *multicast* — send each record to all N live readers at once — and to slow down for a reader that falls behind (backpressure); neither is built yet. An out-of-group consumer instead reads a normal materialized copy the producer also writes, which it consumes after the group finishes like any regular shuffle output (§7). Until multicast exists, v1 rejects a pipelined producer with more than one consuming stage. |
| Reliable RDD checkpoint in a member's within-stage chain | incompatible | Reliable `RDD.checkpoint()` writes the RDD to durable storage and cuts its lineage. Two problems: (1) that durable snapshot can be read by a later job, reintroducing the cross-time reuse a transient edge forbids (§4); and (2) the checkpoint is written by a *separate job that runs after the main one succeeds and recomputes the RDD from scratch* — but a member's input is the transient pipelined shuffle, which is already gone by then, so the recompute cannot read it. Rejected at group creation by walking the within-stage chain and failing on any RDD whose `checkpointData` is a `ReliableRDDCheckpointData` (checked via `checkpointData`, not `isCheckpointed`, since the write has not happened yet). Cache / `.persist()` / local checkpoint keep whole partitions in ephemeral storage and are safe — not rejected. |
| Members with differing resource profiles | incompatible | The gang slot check compares one demand against one capacity (§4); v1 requires a group to be single-profile. Per-profile accounting is a follow-up. |
| Adaptive Query Execution over a pipelined exchange | incompatible | AQE reshapes exchanges from complete map-output statistics, which are unavailable while the shuffle is read incrementally. Enforced where exchanges are marked pipelined. |
| Regular shuffle internal to a group (should not occur) | invariant | Split by construction into separate groups (§3); checked and rejected as an inconsistency if it ever arises (§7). |

The AQE row forbids AQE's *intra-batch, mid-stage* replanning — reading a running producer's statistics
within one execution and reshaping its consumers — not statistics-driven adaptivity in general. A
streaming consumer may still reshape a later batch's plan from an earlier batch's completed statistics:
those statistics are final (the earlier batch fully materialized), and the reshaping happens at planning
time, before the later batch's stages are co-scheduled — so it composes with pipelining rather than
contradicting it. That cross-batch feedback is the compatible way to get AQE-like benefits (partition
coalescing, skew handling, join-strategy selection) in a pipelined streaming query.
