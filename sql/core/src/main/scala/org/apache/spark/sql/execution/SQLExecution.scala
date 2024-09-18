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

import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Future => JFuture}
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{ErrorMessageFormat, JobArtifactSet, SparkContext, SparkEnv, SparkException, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.SparkContext.{SPARK_JOB_DESCRIPTION, SPARK_JOB_INTERRUPT_ON_CANCEL}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{SPARK_DRIVER_PREFIX, SPARK_EXECUTOR_PREFIX}
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.SQL_EVENT_TRUNCATE_LENGTH
import org.apache.spark.util.Utils

object SQLExecution extends Logging {

  val EXECUTION_ID_KEY = "spark.sql.execution.id"
  val EXECUTION_ROOT_ID_KEY = "spark.sql.execution.root.id"

  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()

  def getQueryExecution(executionId: Long): QueryExecution = {
    executionIdToQueryExecution.get(executionId)
  }

  private val testing = sys.props.contains(IS_TESTING.key)

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
    val executionId = SQLExecution.nextExecutionId
    sc.setLocalProperty(EXECUTION_ID_KEY, executionId.toString)
    // Track the "root" SQL Execution Id for nested/sub queries. The current execution is the
    // root execution if the root execution ID is null.
    // And for the root execution, rootExecutionId == executionId.
    if (sc.getLocalProperty(EXECUTION_ROOT_ID_KEY) == null) {
      sc.setLocalProperty(EXECUTION_ROOT_ID_KEY, executionId.toString)
    }
    val rootExecutionId = sc.getLocalProperty(EXECUTION_ROOT_ID_KEY).toLong
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
          jobGroupId = Option(sc.getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID))
        )
        try {
          body match {
            case Left(e) =>
              sc.listenerBus.post(startEvent)
              throw e
            case Right(f) =>
              val planDescriptionMode =
                ExplainMode.fromString(sparkSession.sessionState.conf.uiExplainMode)
              val planDesc = queryExecution.explainString(planDescriptionMode)
              val planInfo = try {
                SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan)
              } catch {
                case NonFatal(e) =>
                  logDebug("Failed to generate SparkPlanInfo", e)
                  // If the queryExecution already failed before this, we are not able to generate
                  // the the plan info, so we use and empty graphviz node to make the UI happy
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
          val endTime = System.nanoTime()
          val errorMessage = ex.map {
            case e: SparkThrowable =>
              SparkThrowableHelper.getMessage(e, ErrorMessageFormat.PRETTY)
            case e =>
              Utils.exceptionString(e)
          }
          if (queryExecution.shuffleCleanupMode != DoNotCleanup
            && isExecutedPlanAvailable) {
            val shuffleIds = queryExecution.executedPlan match {
              case ae: AdaptiveSparkPlanExec =>
                ae.context.shuffleIds.asScala.keys
              case _ =>
                Iterable.empty
            }
            shuffleIds.foreach { shuffleId =>
              queryExecution.shuffleCleanupMode match {
                case RemoveShuffleFiles =>
                  // Same as what we do in ContextCleaner.doCleanupShuffle, but do not unregister
                  // the shuffle on MapOutputTracker, so that stage retries would be triggered.
                  // Set blocking to Utils.isTesting to deflake unit tests.
                  sc.shuffleDriverComponents.removeShuffle(shuffleId, Utils.isTesting)
                case SkipMigration =>
                  SparkEnv.get.blockManager.migratableResolver.addShuffleToSkip(shuffleId)
                case _ => // this should not happen
              }
            }
          }
          val event = SparkListenerSQLExecutionEnd(
            executionId,
            System.currentTimeMillis(),
            // Use empty string to indicate no error, as None may mean events generated by old
            // versions of Spark.
            errorMessage.orElse(Some("")))
          // Currently only `Dataset.withAction` and `DataFrameWriter.runCommand` specify the `name`
          // parameter. The `ExecutionListenerManager` only watches SQL executions with name. We
          // can specify the execution name in more places in the future, so that
          // `QueryExecutionListener` can track more cases.
          event.executionName = name
          event.duration = endTime - startTime
          event.qe = queryExecution
          event.executionFailure = ex
          sc.listenerBus.post(event)
        }
      }
    } finally {
      executionIdToQueryExecution.remove(executionId)
      sc.setLocalProperty(EXECUTION_ID_KEY, oldExecutionId)
      // Unset the "root" SQL Execution Id once the "root" SQL execution completes.
      // The current execution is the root execution if rootExecutionId == executionId.
      if (sc.getLocalProperty(EXECUTION_ROOT_ID_KEY) == executionId.toString) {
        sc.setLocalProperty(EXECUTION_ROOT_ID_KEY, null)
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
      try {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId)
        body
      } finally {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, oldExecutionId)
      }
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

  /**
   * Wrap passed function to ensure necessary thread-local variables like
   * SparkContext local properties are forwarded to execution thread
   */
  def withThreadLocalCaptured[T](
      sparkSession: SparkSession, exec: ExecutorService) (body: => T): JFuture[T] = {
    val activeSession = sparkSession
    val sc = sparkSession.sparkContext
    val localProps = Utils.cloneProperties(sc.getLocalProperties)
    val artifactState = JobArtifactSet.getCurrentJobArtifactState.orNull
    exec.submit(() => JobArtifactSet.withActiveJobArtifactState(artifactState) {
      val originalSession = SparkSession.getActiveSession
      val originalLocalProps = sc.getLocalProperties
      SparkSession.setActiveSession(activeSession)
      sc.setLocalProperties(localProps)
      val res = body
      // reset active session and local props.
      sc.setLocalProperties(originalLocalProps)
      if (originalSession.nonEmpty) {
        SparkSession.setActiveSession(originalSession.get)
      } else {
        SparkSession.clearActiveSession()
      }
      res
    })
  }
}
