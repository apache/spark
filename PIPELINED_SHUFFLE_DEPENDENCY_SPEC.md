# Pipelined Shuffle Dependency & Concurrent Stage Scheduling

A spec for running data-dependent stages of a single job concurrently, connected by a shuffle the
consumer reads incrementally.

---

## 1. Motivation

Today a multi-stage job runs one stage at a time: each shuffle is fully materialized before the next
stage starts. Some workloads need the stages of a single job to run **concurrently**, connected by a
shuffle whose consumer reads the producer's output **as it is produced** rather than after the
producer finishes. This spec introduces the scheduler primitives to express and run that.

"Run these stages concurrently" and "the connecting shuffle is incremental" are the same decision
seen from two sides: co-scheduling a producer and consumer is only useful if the edge is readable
before the producer completes.

---

## 2. Primitives

### 2.1 Pipelined shuffle dependency (PSD)

A shuffle dependency declared **incrementally readable**: a consumer stage may begin reading its
output while the producer stage is still running.

- It is a shuffle dependency (has a `shuffleId`, partitioner, map/reduce sides); the *pipelined*
  property is a binding part of the scheduler contract, not an advisory hint.
- The property is set during **physical planning** (an execution concern, not a logical-plan one)
  and carried into the `ShuffleDependency` the `DAGScheduler` reads at stage-creation time.
- It is also the **per-dependency selector** for the shuffle implementation: the shuffle layer maps a
  pipelined dependency to an incremental `ShuffleManager` and everything else to the default, so one
  job with both regular and pipelined groups uses the right implementation for each — selected
  per-dependency, not per-job. The scheduler construct stays generic; the shuffle implementation
  stays pluggable.
  - For example, an additional conf (e.g. `spark.shuffle.manager.incremental`) can be introduced to
    specify the incremental shuffle implementation used for pipelined shuffle dependencies, alongside
    the existing `spark.shuffle.manager` for the default.

A **regular shuffle dependency (RSD)** is an ordinary shuffle dependency: its output must be fully
materialized before any consumer reads it.

Note: the name *pipelined* is deliberately chosen over *streaming*. The property is that a consumer
reads producer output as it is produced — software-pipelining of dependent stages — which is a
general execution capability. Streaming / real-time mode is the first caller, but nothing about the
primitive is streaming-specific, so the dependency and the group are named for the capability, not
the caller.

### 2.2 Pipelined group (G)

The set of stages connected to one another through pipelined edges — the connected component of the
stage DAG when only pipelined edges are considered.

- A stage with no incident pipelined edge is a **singleton group** and behaves exactly as a normal
  stage today.
- The group — not the edge or the individual stage — is the unit of **admission**, **slot
  checking**, **completion**, and **failure**.

**External input of G:** a regular shuffle dependency whose consumer is in G and whose producer is
not — i.e. a normal materialized parent of the group.

---

## 3. Group formation

- **Stage decomposition is unchanged.** A pipelined dependency introduces a shuffle boundary exactly
  as a regular one does; the set of stages and their partitioning are identical. The pipelined
  property changes only *when* stages run relative to one another, never *how the plan is cut into
  stages*.
- **Group = connected component over pipelined edges.** As stages are created, two stages joined by a
  pipelined edge are placed in the same group; the group is the transitive closure.
- **Every stage belongs to exactly one group** (singletons included). Group membership is fixed at
  stage-creation time.

---

## 4. Scheduling & admission

- **A pipelined edge is non-sequencing.** The consumer of a pipelined dependency does not wait for
  its producer to materialize. (A regular-dependency consumer still waits — the default behavior.)
- **Group readiness.** A group is ready to be admitted when every external input of the group (its
  regular materialized parents) is available — the same precondition a normal stage has today, lifted
  to the group. Pipelined parents inside the group impose no readiness precondition.
- **Gang admission (all-or-nothing).** A pipelined group is admitted only if the cluster can currently
  run all tasks of all member stages **concurrently**; admission then submits every member stage at
  once. There is no partial admission — a pipelined group is never left with some members running
  while others wait on slots the running members occupy.
  - *A non-pipelined (singleton) group is unaffected:* it is admitted exactly as a normal stage is
    today — submitted when its parents are available, filling slots incrementally, with no all-at-once
    requirement. Gang admission applies only to a group of two or more stages connected by pipelined
    edges.
- **Slot check.** The group's aggregate concurrent-task demand — the sum of `numTasks` over member
  stages — is compared against the number of available slots in the cluster (a slot is one task's
  worth of capacity, so this is the maximum number of tasks that can run at once). If the group
  needs more slots than the cluster can offer, the submission fails fast, since the group could
  never become fully co-resident.
  - *What "available slots" is:* the cluster's concurrent-task capacity — the number of tasks it can
    run at once — reusing the value the barrier slot check already uses (`sc.maxNumConcurrentTasks`),
    not `spark.default.parallelism` (a default partition count, unrelated to how many tasks can run
    concurrently).
- **Single resource profile per group (v1).** A resource profile is the executor/task resource
  requirement (cores, memory, GPUs, ...) a stage runs under; the number of concurrent slots is
  defined *per profile* (a cluster may run many concurrent CPU tasks but few GPU tasks). Comparing
  one demand against one capacity is therefore only well-defined when all member stages of a group
  share a single profile. v1 requires that and rejects a mixed-profile group (fail-fast, §9);
  per-profile accounting — checking each profile's demand against that profile's own capacity — is a
  follow-up, not needed for the streaming shapes whose members share the default profile.
- **Co-residency.** Once admitted, all member stages of a group are simultaneously running.
- **Single ownership.** A pipelined group belongs to exactly one job; a pipelined producer is not
  shared across jobs.

### 4.1 Cross-group admission (multiple groups / groups vs. regular jobs)

A pipelined group holds its slots for its whole run, so admission is decided against currently-free
slots, and a group that does not fit is failed rather than queued.

- **Capacity is free slots, not total.** The slot check counts only slots not occupied by running
  tasks — running groups' and regular jobs' tasks are subtracted — so a group is admitted only if its
  full demand fits in the slots free at admission time.
  - *How free capacity is measured:* the cluster's concurrent-task capacity (the number of tasks it
    can run at once — what an all-at-once gang admission must check against, see §4) minus the tasks
    currently running.
- **No waiting queue, no partial reservation.** A group that doesn't fit fails its admission; it never
  sits in a queue holding slots incrementally. This keeps the scheduler change minimal and cannot
  deadlock (a group never occupies slots a sibling is blocked on), matching barrier, which also fails
  its slot check rather than queuing.
- **Two failure reasons, one path.** Demand > total capacity can never fit (a plan/sizing error);
  demand > free slots but ≤ total could fit later. Both fail admission.
- **Retry is the caller's decision.** The scheduler does not automatically retry a failed admission —
  it fails the job. Whether and how to retry is up to the caller. For a streaming query this is the
  batch-execution loop restarting the batch (with its own backoff), so a transiently-busy cluster is
  handled by the caller's restart policy, not a scheduler retry loop.

---

## 5. Completion

- **Group-observable completion.** A member stage is not job-observably finished until **all** member
  stages of its group have completed successfully. A result stage's job therefore completes only when
  its entire group has, and a member that finishes ahead of the others cannot advance job or stage
  completion until the group does.
- **Defer the finish decision, not per-task work.** When a member finishes before its group, only its
  stage-finish / job-finish transition is deferred until group completion. Per-task side effects that
  must always run — accumulator updates, output-commit coordination, task-end listener events — run
  immediately.
- **Replay window.** There is a window between a member finishing and group completion. A failure in
  that window is a group failure (§6): the deferred finish transitions are dropped and the group
  reruns.
- **In-group result-stage side effects must be idempotent.** Per-task side effects run immediately
  (above), including a result stage's output commit. If a result task commits and a sibling then
  fails in the replay window, the group reruns and re-delivers that output. This is the standard
  streaming model — a batch is re-delivered on recovery and the sink must absorb it — so v1 requires
  an in-group result stage's side effects to be idempotent, and fail-fast rejects a sink that cannot
  absorb re-delivery (§9). Deferring the commit itself to group completion is a possible future
  augmentation if a non-idempotent committer ever needs to be supported.

### 5.1 Observable completion events (listener bus)

The listener bus is an external contract that monitoring tools depend on, so it is worth stating
exactly when each event is delivered. The rule follows directly from atomic commit: **task-level
events flow in real time, but stage-completion and job-completion events are held until the group
commits — so a listener never observes a member as *successfully completed* before the group as a
whole has.**

| Event | Timing | Rationale |
|-------|--------|-----------|
| `SparkListenerTaskStart` / `SparkListenerTaskEnd` | Real time, as they occur | Per-task facts are true when they happen; a group's members genuinely run concurrently. Deferring these would freeze a member's live progress and metrics for the whole group's duration. Note a successful `TaskEnd` means "this task finished," not "its output is committed" — already true in Spark, since a stage attempt can later be discarded. |
| `SparkListenerStageSubmitted` | Real time, at group admission | All member stages are submitted together (§4); a monitor should show them active simultaneously. |
| `SparkListenerStageCompleted` | Deferred to group commit; on group failure, emitted with a failure reason | "Completed" should track commit, which is atomic at the group level. A member whose tasks finish early is reported as still running until the group commits — which matches the truth that its results are not usable until then. This avoids emitting a success-shaped completion for an attempt that a later group failure would discard. |
| `SparkListenerJobEnd(JobSucceeded)` | At group commit only | Job completion delivers results to the caller and cancels sibling stages; emitting it before the group commits risks double/inconsistent result delivery if the group later fails. Non-negotiable. |
| `SparkListenerJobEnd(JobFailed)` | On group failure | Group-atomic failure (§6): buffered success transitions are dropped, never replayed as success. |

Failure-path consequence: because stage- and job-completion are deferred (never emitted early as
success), a group failure needs only to emit them as *failure* — there is no premature success event
to retract. Already-emitted task events stand as-is (they were true), consistent with how Spark
treats task events from a stage attempt that is later discarded.

Open question for reviewers: whether `SparkListenerStageCompleted` for a member should be deferred to
group commit (above) or emitted in real time when the member's tasks finish. Deferring keeps the
listener's notion of "completed" consistent with the atomic-commit model at the cost of an inflated
stage duration on the timeline; real-time emission gives crisper per-stage timings but reports a
member as completed before the group has committed.

---

## 6. Failure

- **Failure is group-atomic.** Any member task failure, for any reason, fails the whole group; there
  is no independent single-stage failure within a group.
  - *Single-stage resubmit is disabled for group members.* That isolated-resubmit branch is turned
    off for a member of a pipelined group: the transient pipelined shuffle cannot be re-read and
    members are co-scheduled, so a lone-stage resubmit is never valid. With it disabled, every member
    failure necessarily routes to group failure.
    - *Note this differs from the base scheduler,* which handles a task failure by resubmitting just
      that one stage — the `FetchFailed` -> resubmit path, and retry paths generally — recomputing
      the failed stage in isolation and resuming.
  - *Teardown is by group membership, not producer availability.* At the end of a batch the producer
    finishes and registers all its map outputs — becoming "available" — while the consumer is still
    draining the remainder, so a producer-available-while-consumer-still-running window is normal,
    not rare. For a transient pipelined shuffle, "available" does not mean the consumer is safe: the
    data is not durably stored and the consumer is still actively reading it, so a producer failure
    in that window still strands the consumer. Failure propagation that keys off producer
    availability would miss the consumer here; teardown is therefore keyed on group membership, so a
    producer failure always tears down its co-scheduled consumers regardless of the producer's
    availability at the failure instant.
- **Recovery.** Group failure tears down all member stages atomically and discards their deferred
  completions. Because the shuffle is transient it cannot be partially recovered; the job fails and
  the caller re-runs it.

---

## 7. Interaction with regular dependencies

- **Regular shuffle into a group — supported.** A regular dependency from outside the group to a
  member is a normal materialized parent (an external input); the group waits for it.
  - A lost external durable input reruns the group rather than being recomputed in isolation.
- **Regular shuffle out of a group — supported.** A regular dependency from a member to a stage
  outside the group produces a materialized output that a downstream group consumes with normal
  sequencing; the downstream waits for the group to complete.
- **Regular shuffle internal to a group — fail-fast / unsupported.** If a regular dependency's
  producer and consumer both fall in the same group, the group would co-schedule them concurrently
  while the regular edge demands the producer be materialized first — a contradiction. Reject at
  stage creation.
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

The rejected idioms fall into two kinds. Some are **moot** under the group failure model (§6):
Spark's stage-level recompute/rollback mechanisms never fire inside a group, because any failure
aborts the whole group and the caller reruns from scratch — so a mechanism whose only job is to
recompute or roll back a stage after a partial failure is never reached. We reject these rather than
carry dead, self-contradictory machinery. The rest are **incompatible** with concurrent execution
itself and would corrupt or hang a group that never fails.

| Rejected condition | Kind | Why rejected |
|--------------------|------|--------------|
| Barrier execution in a member stage | incompatible | Barrier exposes output only after a global sync, contradicting concurrent partial reads. |
| Dynamic resource allocation | incompatible | Gang admission needs a stable slot set; reclaiming executors from a pinned-open group can deadlock it. |
| Speculative execution | incompatible | A speculative producer copy races a consumer already reading partial output; no commit barrier protects the read. |
| Push-based shuffle merge | incompatible | Push-based shuffle merges each partition's blocks into large pre-sealed files on merger services, and those files become readable only after a post-map-completion "finalize" step — the exact opposite of a pipelined shuffle, whose consumer reads before the producer finishes, so the two can never apply to the same edge. |
| Statically-indeterminate producer | moot | Its recovery is stage rollback-and-recompute, which a group never performs (§6: any failure aborts the group). The mechanism is never reached; rejected so the inapplicability is explicit rather than latent. (A producer whose output RDD is classified `INDETERMINATE` — a rerun can yield different data, not just a different order — determinable from the RDD graph at stage-creation time.) |
| Checksum-mismatch full retry | moot | The runtime counterpart to static indeterminism: it checksums each map task's output and, on a cross-attempt mismatch, rolls back and re-runs the succeeding stages. A group never keeps succeeding stages across a retry (§6), so this never fires; rejected to keep the inapplicability explicit. |
| Cached/persisted RDD in a member's within-stage chain | incompatible | Would capture partial output read mid-run and serve it as a complete result on reuse. Scoped to the member's within-stage (narrow-dependency) chain: a cached *complete* input reached via a broadcast variable or across a materialized shuffle — e.g. the static side of a stream-static join — is outside that chain and is unaffected. |
| Pipelined producer shared across jobs | incompatible | Breaks single ownership: a failure must map to exactly one job's group. |
| Regular shuffle internal to a group | incompatible | Concurrent co-scheduling contradicts materialize-before-read (§7). |
| Members with differing resource profiles | incompatible | The gang slot check compares one demand against one capacity (§4); v1 requires a group to be single-profile. Per-profile accounting is a follow-up. |
| Adaptive Query Execution over a pipelined exchange | incompatible | AQE reshapes exchanges from complete map-output statistics, which are unavailable while the shuffle is read incrementally. Enforced where exchanges are marked pipelined. |

The AQE row forbids AQE's *intra-batch, mid-stage* replanning — reading a running producer's statistics
within one execution and reshaping its consumers — not statistics-driven adaptivity in general. A
streaming consumer may still reshape a later batch's plan from an earlier batch's completed statistics:
those statistics are final (the earlier batch fully materialized), and the reshaping happens at planning
time, before the later batch's stages are co-scheduled — so it composes with pipelining rather than
contradicting it. That cross-batch feedback is the compatible way to get AQE-like benefits (partition
coalescing, skew handling, join-strategy selection) in a pipelined streaming query.
