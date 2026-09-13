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

package org.apache.spark.sql.execution

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ExecutorService}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{ErrorMessageFormat, JobArtifactSet, JobArtifactState, SparkContext, SparkEnv, SparkException, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.SparkContext.{SPARK_JOB_DESCRIPTION, SPARK_JOB_INTERRUPT_ON_CANCEL}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{EXECUTION_ID, SHUFFLE_ID}
import org.apache.spark.internal.config.{SPARK_DRIVER_PREFIX, SPARK_EXECUTOR_PREFIX}
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.SQL_EVENT_TRUNCATE_LENGTH
import org.apache.spark.util.{Utils, UUIDv7Generator}

/**
 * Captures SQL-specific thread-local variables so they can be restored on a different thread.
 * Use [[SQLExecution.captureThreadLocals]] to create an instance on the originating thread,
 * then call [[runWith]] on the target thread to execute a block with these thread locals applied.
 */
case class SQLExecutionThreadLocalCaptured(
  sparkSession: SparkSession,
  localProps: java.util.Properties,
  artifactState: JobArtifactState) {

  /**
   * Run the given body with the captured thread-local variables applied on the current thread.
   * Original thread-local values are saved and restored after the body completes.
   */
  def runWith[T](body: => T): T = {
    val sc = sparkSession.sparkContext
    JobArtifactSet.withActiveJobArtifactState(artifactState) {
      val originalSession = SparkSession.getActiveSession
      val originalLocalProps = sc.getLocalProperties
      SparkSession.setActiveSession(sparkSession)
      val res = SQLExecution.withSessionTagsApplied(sparkSession) {
        sc.setLocalProperties(localProps)
        val res = body
        // reset active session and local props.
        sc.setLocalProperties(originalLocalProps)
        res
      }
      if (originalSession.nonEmpty) {
        SparkSession.setActiveSession(originalSession.get)
      } else {
        SparkSession.clearActiveSession()
      }
      res
    }
  }
}

object SQLExecution extends Logging {

  val EXECUTION_ID_KEY = "spark.sql.execution.id"
  val EXECUTION_ROOT_ID_KEY = "spark.sql.execution.root.id"
  val QUERY_ID_KEY = "spark.sql.execution.query.id"

  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  private[sql] val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()

  def getQueryExecution(executionId: Long): QueryExecution = {
    executionIdToQueryExecution.get(executionId)
  }

  private val testing = sys.props.contains(IS_TESTING.key)

  private[sql] def executionIdJobTag(session: SparkSession, id: Long) =
    s"${session.sessionJobTag}-execution-root-id-$id"

  private[sql] def checkSQLExecutionId(sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext
    // only throw an exception during tests. a missing execution ID should not fail a job.
    if (testing && sc.getLocalProperty(EXECUTION_ID_KEY) == null) {
      // Attention testers: when a test fails with this exception, it means that the action that
      // started execution of a query didn't call withNewExecutionId. The execution ID should be
      // set by calling withNewExecutionId in the action that begins execution, like
      // Dataset.collect or DataFrameWriter.insertInto.
      throw SparkException.internalError("Execution ID should be set")
    }
  }

  private def extractShuffleIds(plan: SparkPlan): Seq[Int] = {
    val shuffleIdsOption = plan.collectFirst {
      case ae: AdaptiveSparkPlanExec =>
        ae.context.shuffleIds.asScala.keys.toSeq
    }
    shuffleIdsOption.getOrElse {
        plan.collect {
          case exec: ShuffleExchangeLike => exec.shuffleId
        }
    }
  }

  /**
   * Best-effort cleanup of the shuffle dependencies produced by `queryExecution`, invoked from
   * `withNewExecutionId0`'s `finally` while the `SparkContext` may be tearing down (`removeShuffle`
   * can reach a stopped `BlockManagerMaster`, `SparkEnv.get` can be null). Each shuffle is cleaned
   * independently and non-fatal failures are logged rather than propagated, so one failure does not
   * abandon the rest; an `InterruptedException` is not `NonFatal` and still propagates. The log is
   * mode-specific: for `RemoveShuffleFiles` a failure may leak the shuffle's files on disk, while
   * `SkipMigration` only marks the shuffle to skip decommission migration and keeps its files by
   * design.
   */
  private def cleanupShuffleDependencies(
      queryExecution: QueryExecution,
      executionId: Long): Unit = {
    val sc = queryExecution.sparkSession.sparkContext
    try {
      val shuffleIds = queryExecution.executedPlan match {
        case command: V2CommandExec =>
          command.children.flatMap(extractShuffleIds)
        case dataWritingCommand: DataWritingCommandExec =>
          extractShuffleIds(dataWritingCommand.child)
        case plan =>
          extractShuffleIds(plan)
      }
      shuffleIds.foreach { shuffleId =>
        queryExecution.shuffleCleanupMode match {
          case RemoveShuffleFiles =>
            try {
              // Same as ContextCleaner.doCleanupShuffle, but do not unregister the shuffle on
              // MapOutputTracker so that stage retries would be triggered. Blocking is
              // Utils.isTesting to deflake unit tests.
              sc.shuffleDriverComponents.removeShuffle(shuffleId, Utils.isTesting)
            } catch {
              case NonFatal(e) =>
                logWarning(log"Failed to remove shuffle ${MDC(SHUFFLE_ID, shuffleId)} for " +
                  log"execution ${MDC(EXECUTION_ID, executionId)}; its files may be left " +
                  log"on disk.", e)
            }
          case SkipMigration =>
            try {
              SparkEnv.get.blockManager.migratableResolver.addShuffleToSkip(shuffleId)
            } catch {
              case NonFatal(e) =>
                logWarning(log"Failed to mark shuffle ${MDC(SHUFFLE_ID, shuffleId)} to skip " +
                  log"migration for execution ${MDC(EXECUTION_ID, executionId)}.", e)
            }
          case _ => // this should not happen
        }
      }
    } catch {
      // `queryExecution.executedPlan` re-throws the planning failure cached in its `LazyTry`.
      case NonFatal(e) =>
        logWarning(log"Failed to clean up shuffle dependencies for execution " +
          log"${MDC(EXECUTION_ID, executionId)}.", e)
    }
  }

  private[sql] val NONE_EXPLAIN_MODE = "none"

  private[sql] val NO_PLAN_DESCRIPTION =
    s"No plan description because ${SQLConf.UI_EXPLAIN_MODE.key}=$NONE_EXPLAIN_MODE"

  /**
   * Returns the plan description carried by the SQL UI events, or a placeholder when the UI
   * explain mode is `none`, in which case the explain string, which is expensive to build for
   * large plans, is not generated at all.
   *
   * `none` is intentionally not an [[ExplainMode]]: it only makes sense for the UI events, and
   * keeping it here avoids making `df.explain("none")` a valid public API.
   */
  private[sql] def planDescription(qe: QueryExecution, uiExplainMode: String): String = {
    if (uiExplainMode.equalsIgnoreCase(NONE_EXPLAIN_MODE)) {
      NO_PLAN_DESCRIPTION
    } else {
      qe.explainString(ExplainMode.fromString(uiExplainMode))
    }
  }

  /**
   * Wrap an action that will execute "queryExecution" to track all Spark jobs in the body so that
   * we can connect them with an execution.
   */
  private def withNewExecutionId0[T](
      queryExecution: QueryExecution,
      name: Option[String] = None)(
      body: Either[Throwable, () => T]): T = queryExecution.sparkSession.withActive {
    val sparkSession = queryExecution.sparkSession
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(EXECUTION_ID_KEY)
    val oldQueryId = sc.getLocalProperty(QUERY_ID_KEY)
    val executionId = SQLExecution.nextExecutionId
    // Use the original queryId for the first execution, generate new ones for
    // subsequent executions
    val queryId = if (queryExecution.firstExecution.compareAndSet(true, false)) {
      queryExecution.queryId
    } else {
      UUIDv7Generator.generate()
    }
    sc.setLocalProperty(EXECUTION_ID_KEY, executionId.toString)
    sc.setLocalProperty(QUERY_ID_KEY, queryId.toString)
    // Track the "root" SQL Execution Id for nested/sub queries. The current execution is the
    // root execution if the root execution ID is null.
    // And for the root execution, rootExecutionId == executionId.
    val existingRootId = sc.getLocalProperty(EXECUTION_ROOT_ID_KEY)
    val rootExecutionId = if (existingRootId != null) {
      existingRootId.toLong
    } else {
      sc.setLocalProperty(EXECUTION_ROOT_ID_KEY, executionId.toString)
      sc.addJobTag(executionIdJobTag(sparkSession, executionId))
      executionId
    }
    executionIdToQueryExecution.put(executionId, queryExecution)
    val originalInterruptOnCancel = sc.getLocalProperty(SPARK_JOB_INTERRUPT_ON_CANCEL)
    if (originalInterruptOnCancel == null) {
      val interruptOnCancel = sparkSession.sessionState.conf.getConf(SQLConf.INTERRUPT_ON_CANCEL)
      sc.setInterruptOnCancel(interruptOnCancel)
    }
    try {
      // sparkContext.getCallSite() would first try to pick up any call site that was previously
      // set, then fall back to Utils.getCallSite(); call Utils.getCallSite() directly on
      // streaming queries would give us call site like "run at <unknown>:0"
      val callSite = sc.getCallSite()

      val truncateLength = sc.conf.get(SQL_EVENT_TRUNCATE_LENGTH)

      val desc = Option(sc.getLocalProperty(SPARK_JOB_DESCRIPTION))
        .filter(_ => truncateLength > 0)
        .map { sqlStr =>
          val redactedStr = Utils
            .redact(sparkSession.sessionState.conf.stringRedactionPattern, sqlStr)
          redactedStr.substring(0, Math.min(truncateLength, redactedStr.length))
        }.getOrElse(callSite.shortForm)

      val globalConfigs = sparkSession.sharedState.conf.getAll.toMap
      val modifiedConfigs = sparkSession.sessionState.conf.getAllConfs
        .filterNot { case (key, value) =>
          key.startsWith(SPARK_DRIVER_PREFIX) ||
            key.startsWith(SPARK_EXECUTOR_PREFIX) ||
            globalConfigs.get(key).contains(value)
        }
      val redactedConfigs = sparkSession.sessionState.conf.redactOptions(modifiedConfigs)

      withSQLConfPropagated(sparkSession) {
        sparkSession.artifactManager.withResources {
          withSessionTagsApplied(sparkSession) {
            var ex: Option[Throwable] = None
            var isExecutedPlanAvailable = false
            val startTime = System.nanoTime()
            val startEvent = SparkListenerSQLExecutionStart(
              executionId = executionId,
              rootExecutionId = Some(rootExecutionId),
              description = desc,
              details = callSite.longForm,
              physicalPlanDescription = "",
              sparkPlanInfo = SparkPlanInfo.EMPTY,
              time = System.currentTimeMillis(),
              modifiedConfigs = redactedConfigs,
              jobTags = sc.getJobTags(),
              jobGroupId = Option(sc.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID)),
              queryId = Some(queryId)
            )
            try {
              body match {
                case Left(e) =>
                  sc.listenerBus.post(startEvent)
                  throw e
                case Right(f) =>
                  val planDesc = planDescription(
                    queryExecution, sparkSession.sessionState.conf.uiExplainMode)
                  val planInfo = try {
                    SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan)
                  } catch {
                    case NonFatal(e) =>
                      logDebug("Failed to generate SparkPlanInfo", e)
                      // If the queryExecution already failed before this, we are not able to
                      // generate the the plan info, so we use and empty graphviz node to make the
                      // UI happy
                      SparkPlanInfo.EMPTY
                  }
                  sc.listenerBus.post(
                    startEvent.copy(physicalPlanDescription = planDesc, sparkPlanInfo = planInfo))
                  isExecutedPlanAvailable = true
                  f()
              }
            } catch {
              case e: Throwable =>
                ex = Some(e)
                throw e
            } finally {
              // `SparkContext.stop()` nulls `dagScheduler` before it stops the listener bus, so the
              // end event may still be posted after the scheduler is unavailable. Keep observation
              // completion in a `finally` so an error in this block never leaves a waiter hung.
              try {
                val endTime = System.nanoTime()
                val errorMessage = ex.map { e =>
                  try {
                    e match {
                      case st: SparkThrowable =>
                        SparkThrowableHelper.getMessage(st, ErrorMessageFormat.PRETTY)
                      case _ =>
                        Utils.exceptionString(e)
                    }
                  } catch {
                    // Rendering a user throwable can itself throw (e.g. a custom `getMessage`).
                    // Fall back to a safe value so the query's real failure is still surfaced and
                    // the cleanup, event post and observation completion below still run.
                    case NonFatal(t) =>
                      logWarning(log"Failed to render the error message for execution " +
                        log"${MDC(EXECUTION_ID, executionId)}.", t)
                      e.getClass.getName
                  }
                }
                if (queryExecution.shuffleCleanupMode != DoNotCleanup && isExecutedPlanAvailable) {
                  cleanupShuffleDependencies(queryExecution, executionId)
                }
                val event = SparkListenerSQLExecutionEnd(
                  executionId,
                  System.currentTimeMillis(),
                  // Use empty string to indicate no error, as None may mean events generated by old
                  // versions of Spark.
                  errorMessage.orElse(Some("")),
                  Some(queryId))
                // Currently only `Dataset.withAction` and `DataFrameWriter.runCommand` specify the
                // `name` parameter. The `ExecutionListenerManager` only watches SQL executions with
                // name. We can specify the execution name in more places in the future, so that
                // `QueryExecutionListener` can track more cases.
                event.executionName = name
                event.duration = endTime - startTime
                event.qe = queryExecution
                event.executionFailure = ex
                // Snapshot the `@volatile` `dagScheduler` once and share it across both reads
                // below; it is null once `SparkContext.stop()` has run.
                val dagSchedulerOpt = Option(sc.dagScheduler)
                if (Utils.isTesting) {
                  import scala.jdk.CollectionConverters._
                  // Only runs under `Utils.isTesting`; hits the same teardown race as the job
                  // cleanup below.
                  event.jobIds = dagSchedulerOpt
                    .flatMap(ds => Option(ds.activeQueryToJobs.get(executionId)))
                    .map(_.asScala.map(_.jobId).toSet)
                    .getOrElse(Set.empty)
                }

                // Clean up jobs tracked by DAGScheduler for this query execution.
                dagSchedulerOpt.foreach(_.cleanupQueryJobs(executionId))

                sc.listenerBus.post(event)
              } finally {
                // Complete the observation whatever the block above threw, so an `Observation.get`
                // waiter is never left hung. `promise.tryComplete` is idempotent.
                sparkSession.observationManager.tryComplete(queryExecution)
              }
            }
          }
        }
      }
    } finally {
      executionIdToQueryExecution.remove(executionId)
      sc.setLocalProperty(EXECUTION_ID_KEY, oldExecutionId)
      sc.setLocalProperty(QUERY_ID_KEY, oldQueryId)
      // Unset the "root" SQL Execution Id once the "root" SQL execution completes.
      // The current execution is the root execution if rootExecutionId == executionId.
      if (sc.getLocalProperty(EXECUTION_ROOT_ID_KEY) == executionId.toString) {
        sc.setLocalProperty(EXECUTION_ROOT_ID_KEY, null)
        sc.removeJobTag(executionIdJobTag(sparkSession, executionId))
      }
      sc.setLocalProperty(SPARK_JOB_INTERRUPT_ON_CANCEL, originalInterruptOnCancel)
    }
  }

  def withNewExecutionId[T](
      queryExecution: QueryExecution,
      name: Option[String] = None)(body: => T): T = {
    withNewExecutionId0(queryExecution, name)(Right(() => body))
  }

  def withNewExecutionIdOnError(
      queryExecution: QueryExecution,
      name: Option[String] = None)(t: Throwable): Unit = {
    withNewExecutionId0(queryExecution, name)(Left(t))
  }


  /**
   * Wrap an action with a known executionId. When running a different action in a different
   * thread from the original one, this method can be used to connect the Spark jobs in this action
   * with the known executionId, e.g., `BroadcastExchangeExec.relationFuture`.
   */
  def withExecutionId[T](sparkSession: SparkSession, executionId: String)(body: => T): T = {
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    withSQLConfPropagated(sparkSession) {
      withSessionTagsApplied(sparkSession) {
        try {
          sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId)
          body
        } finally {
          sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, oldExecutionId)
        }
      }
    }
  }

  private[sql] def withSessionTagsApplied[T](sparkSession: SparkSession)(block: => T): T = {
    val allTags = sparkSession.managedJobTags.get().values.toSet + sparkSession.sessionJobTag
    sparkSession.sparkContext.addJobTags(allTags)

    try {
      block
    } finally {
      sparkSession.sparkContext.removeJobTags(allTags)
    }
  }

  /**
   * Wrap an action with specified SQL configs. These configs will be propagated to the executor
   * side via job local properties.
   */
  def withSQLConfPropagated[T](sparkSession: SparkSession)(body: => T): T = {
    val sc = sparkSession.sparkContext
    // Set all the specified SQL configs to local properties, so that they can be available at
    // the executor side.
    val allConfigs = sparkSession.sessionState.conf.getAllConfs
    val originalLocalProps = allConfigs.collect {
      case (key, value) if key.startsWith("spark") =>
        val originalValue = sc.getLocalProperty(key)
        sc.setLocalProperty(key, value)
        (key, originalValue)
    }

    try {
      body
    } finally {
      for ((key, value) <- originalLocalProps) {
        sc.setLocalProperty(key, value)
      }
    }
  }

  def captureThreadLocals(sparkSession: SparkSession): SQLExecutionThreadLocalCaptured = {
    val sc = sparkSession.sparkContext
    val localProps = Utils.cloneProperties(sc.getLocalProperties)
    // `getCurrentJobArtifactState` will return a stat only in Spark Connect mode. In non-Connect
    // mode, we default back to the resources of the current Spark session.
    val artifactState =
      JobArtifactSet.getCurrentJobArtifactState.getOrElse(sparkSession.artifactManager.state)
    SQLExecutionThreadLocalCaptured(sparkSession, localProps, artifactState)
  }

  /**
   * Wrap passed function to ensure necessary thread-local variables like
   * SparkContext local properties are forwarded to execution thread
   */
  def withThreadLocalCaptured[T](
      sparkSession: SparkSession, exec: ExecutorService) (body: => T): CompletableFuture[T] = {
    val threadLocalCaptured = captureThreadLocals(sparkSession)
    CompletableFuture.supplyAsync(() => {
      threadLocalCaptured.runWith(body)
    }, exec)
  }
}
