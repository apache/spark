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

package org.apache.spark.scheduler.cluster

import java.util.EnumSet
import java.util.concurrent.atomic.AtomicBoolean
import javax.servlet.DispatcherType

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}

import org.apache.spark.SparkContext
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.UI._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.storage.{BlockManagerId, BlockManagerMaster}
import org.apache.spark.util.{RpcUtils, ThreadUtils, Utils}

/**
 * Abstract Yarn scheduler backend that contains common logic
 * between the client and cluster Yarn scheduler backends.
 */
private[spark] abstract class YarnSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  private val stopped = new AtomicBoolean(false)

  override val minRegisteredRatio =
    if (conf.get(config.SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO).isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  protected var totalExpectedExecutors = 0

  protected val yarnSchedulerEndpoint = new YarnSchedulerEndpoint(rpcEnv)
  protected var amEndpoint: Option[RpcEndpointRef] = None

  private val yarnSchedulerEndpointRef = rpcEnv.setupEndpoint(
    YarnSchedulerBackend.ENDPOINT_NAME, yarnSchedulerEndpoint)

  private implicit val askTimeout = RpcUtils.askRpcTimeout(sc.conf)

  /**
   * Declare implicit single thread execution context for futures doRequestTotalExecutors and
   * doKillExecutors below, avoiding using the global execution context that may cause conflict
   * with user code's execution of futures.
   */
  private implicit val schedulerEndpointEC = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonSingleThreadExecutor("yarn-scheduler-endpoint"))

  /** Application ID. */
  protected var appId: Option[ApplicationId] = None

  /** Attempt ID. This is unset for client-mode schedulers */
  private var attemptId: Option[ApplicationAttemptId] = None

  private val blockManagerMaster: BlockManagerMaster = sc.env.blockManager.master

  private val minMergersThresholdRatio =
    conf.get(config.SHUFFLE_MERGER_LOCATIONS_MIN_THRESHOLD_RATIO)

  private val minMergersStaticThreshold =
    conf.get(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD)

  private val maxNumExecutors = conf.get(config.DYN_ALLOCATION_MAX_EXECUTORS)

  private val numExecutors = conf.get(config.EXECUTOR_INSTANCES).getOrElse(0)

  /**
   * Bind to YARN. This *must* be done before calling [[start()]].
   *
   * @param appId YARN application ID
   * @param attemptId Optional YARN attempt ID
   */
  protected def bindToYarn(appId: ApplicationId, attemptId: Option[ApplicationAttemptId]): Unit = {
    this.appId = Some(appId)
    this.attemptId = attemptId
  }

  protected def startBindings(): Unit = {
    require(appId.isDefined, "application ID unset")
  }

  override def stop(): Unit = {
    try {
      // SPARK-12009: To prevent Yarn allocator from requesting backup for the executors which
      // was Stopped by SchedulerBackend.
      requestTotalExecutors(Map.empty, Map.empty, Map.empty)
      super.stop()
    } finally {
      stopped.set(true)
    }
  }

  /**
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
   * This attempt ID only includes attempt counter, like "1", "2".
   *
   * @return The application attempt id, if available.
   */
  override def applicationAttemptId(): Option[String] = {
    attemptId.map(_.getAttemptId.toString)
  }

  /**
   * Get an application ID associated with the job.
   * This returns the string value of [[appId]] if set, otherwise
   * the locally-generated ID from the superclass.
   * @return The application ID
   */
  override def applicationId(): String = {
    appId.map(_.toString).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }
  }

  private[cluster] def prepareRequestExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): RequestExecutors = {
    val excludedNodes: Set[String] = scheduler.excludedNodes()
    // For locality preferences, ignore preferences for nodes that are excluded
    val filteredRPHostToLocalTaskCount = rpHostToLocalTaskCount.map { case (rpid, v) =>
      (rpid, v.filter { case (host, count) => !excludedNodes.contains(host) })
    }
    RequestExecutors(resourceProfileToTotalExecs, numLocalityAwareTasksPerResourceProfileId,
      filteredRPHostToLocalTaskCount, excludedNodes)
  }

  /**
   * Request executors from the ApplicationMaster by specifying the total number desired.
   * This includes executors already pending or running.
   */
  override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    yarnSchedulerEndpointRef.ask[Boolean](prepareRequestExecutors(resourceProfileToTotalExecs))
  }

  /**
   * Request that the ApplicationMaster kill the specified executors.
   */
  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    yarnSchedulerEndpointRef.ask[Boolean](KillExecutors(executorIds))
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= totalExpectedExecutors * minRegisteredRatio
  }

  override def getShufflePushMergerLocations(
      numPartitions: Int,
      resourceProfileId: Int): Seq[BlockManagerId] = {
    // TODO (SPARK-33481) This is a naive way of calculating numMergersDesired for a stage,
    // TODO we can use better heuristics to calculate numMergersDesired for a stage.
    val maxExecutors = if (Utils.isDynamicAllocationEnabled(sc.getConf)) {
      maxNumExecutors
    } else {
      numExecutors
    }
    val tasksPerExecutor = sc.resourceProfileManager
      .resourceProfileFromId(resourceProfileId).maxTasksPerExecutor(sc.conf)
    val numMergersDesired = math.min(
      math.max(1, math.ceil(numPartitions / tasksPerExecutor).toInt), maxExecutors)
    val minMergersNeeded = math.max(minMergersStaticThreshold,
      math.floor(numMergersDesired * minMergersThresholdRatio).toInt)

    // Request for numMergersDesired shuffle mergers to BlockManagerMasterEndpoint
    // and if it's less than minMergersNeeded, we disable push based shuffle.
    val mergerLocations = blockManagerMaster
      .getShufflePushMergerLocations(numMergersDesired, scheduler.excludedNodes())
    if (mergerLocations.size < numMergersDesired && mergerLocations.size < minMergersNeeded) {
      Seq.empty[BlockManagerId]
    } else {
      logDebug(s"The number of shuffle mergers desired ${numMergersDesired}" +
        s" and available locations are ${mergerLocations.length}")
      mergerLocations
    }
  }

  /**
   * Add filters to the SparkUI.
   */
  private[cluster] def addWebUIFilter(
      filterName: String,
      filterParams: Map[String, String],
      proxyBase: String): Unit = {
    if (proxyBase != null && proxyBase.nonEmpty) {
      System.setProperty("spark.ui.proxyBase", proxyBase)
    }

    val hasFilter =
      filterName != null && filterName.nonEmpty &&
      filterParams != null && filterParams.nonEmpty
    if (hasFilter) {
      // SPARK-26255: Append user provided filters(spark.ui.filters) with yarn filter.
      val allFilters = Seq(filterName) ++ conf.get(UI_FILTERS)
      logInfo(s"Add WebUI Filter. $filterName, $filterParams, $proxyBase")

      // For already installed handlers, prepend the filter.
      scheduler.sc.ui.foreach { ui =>
        // Lock the UI so that new handlers are not added while this is running. Set the updated
        // filter config inside the lock so that we're sure all handlers will properly get it.
        ui.synchronized {
          filterParams.foreach { case (k, v) =>
            conf.set(s"spark.$filterName.param.$k", v)
          }
          conf.set(UI_FILTERS, allFilters)

          ui.getDelegatingHandlers.foreach { h =>
            h.addFilter(filterName, filterName, filterParams)
            h.prependFilterMapping(filterName, "/*", EnumSet.allOf(classOf[DispatcherType]))
          }
        }
      }
    }
  }

  override def createDriverEndpoint(): DriverEndpoint = {
    new YarnDriverEndpoint()
  }

  /**
   * Reset the state of SchedulerBackend to the initial state. This is happened when AM is failed
   * and re-registered itself to driver after a failure. The stale state in driver should be
   * cleaned.
   */
  override protected[scheduler] def reset(): Unit = {
    super.reset()
    sc.executorAllocationManager.foreach(_.reset())
  }

  override protected def createTokenManager(): Option[HadoopDelegationTokenManager] = {
    Some(new HadoopDelegationTokenManager(sc.conf, sc.hadoopConfiguration, driverEndpoint))
  }

  /**
   * Override the DriverEndpoint to add extra logic for the case when an executor is disconnected.
   * This endpoint communicates with the executors and queries the AM for an executor's exit
   * status when the executor is disconnected.
   */
  private class YarnDriverEndpoint extends DriverEndpoint {

    /**
     * When onDisconnected is received at the driver endpoint, the superclass DriverEndpoint
     * handles it by assuming the Executor was lost for a bad reason and removes the executor
     * immediately.
     *
     * In YARN's case however it is crucial to talk to the application master and ask why the
     * executor had exited. If the executor exited for some reason unrelated to the running tasks
     * (e.g., preemption), according to the application master, then we pass that information down
     * to the TaskSetManager to inform the TaskSetManager that tasks on that lost executor should
     * not count towards a job failure.
     */
    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      addressToExecutorId.get(rpcAddress).foreach { executorId =>
        if (!stopped.get) {
          if (disableExecutor(executorId)) {
            yarnSchedulerEndpoint.handleExecutorDisconnectedFromDriver(executorId, rpcAddress)
          }
        }
      }
    }
  }

  /**
   * An [[RpcEndpoint]] that communicates with the ApplicationMaster.
   */
  protected class YarnSchedulerEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {

    private[YarnSchedulerBackend] def handleExecutorDisconnectedFromDriver(
        executorId: String,
        executorRpcAddress: RpcAddress): Unit = {
      val removeExecutorMessage = amEndpoint match {
        case Some(am) =>
          val lossReasonRequest = GetExecutorLossReason(executorId)
          am.ask[ExecutorLossReason](lossReasonRequest, askTimeout)
            .map { reason => RemoveExecutor(executorId, reason) }(ThreadUtils.sameThread)
            .recover {
              case NonFatal(e) =>
                logWarning(s"Attempted to get executor loss reason" +
                  s" for executor id ${executorId} at RPC address ${executorRpcAddress}," +
                  s" but got no response. Marking as agent lost.", e)
                RemoveExecutor(executorId, ExecutorProcessLost())
            }(ThreadUtils.sameThread)
        case None =>
          logWarning("Attempted to check for an executor loss reason" +
            " before the AM has registered!")
          Future.successful(RemoveExecutor(executorId,
            ExecutorProcessLost("AM is not yet registered.")))
      }

      removeExecutorMessage.foreach { message => driverEndpoint.send(message) }
    }

    private[cluster] def handleClientModeDriverStop(): Unit = {
      amEndpoint match {
        case Some(am) =>
          am.send(Shutdown)
        case None =>
          logWarning("Attempted to send shutdown message before the AM has registered!")
      }
    }

    override def receive: PartialFunction[Any, Unit] = {
      case RegisterClusterManager(am) =>
        logInfo(s"ApplicationMaster registered as $am")
        amEndpoint = Option(am)
        reset()

      case s @ DecommissionExecutorsOnHost(hostId) =>
        logDebug(s"Requesting to decommission host ${hostId}. Sending to driver")
        driverEndpoint.send(s)

      case AddWebUIFilter(filterName, filterParams, proxyBase) =>
        addWebUIFilter(filterName, filterParams, proxyBase)

      case r @ RemoveExecutor(executorId, reason) =>
        if (!stopped.get) {
          logWarning(s"Requesting driver to remove executor $executorId for reason $reason")
          driverEndpoint.send(r)
        }

      // In case of yarn Miscellaneous Process is Spark AM Container
      // Launched for the deploy mode client
      case processInfo @ MiscellaneousProcessAdded(_, _, _) =>
        logDebug(s"Sending the Spark AM info for yarn client mode")
        driverEndpoint.send(processInfo)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case r: RequestExecutors =>
        amEndpoint match {
          case Some(am) =>
            am.ask[Boolean](r).andThen {
              case Success(b) => context.reply(b)
              case Failure(NonFatal(e)) =>
                logError(s"Sending $r to AM was unsuccessful", e)
                context.sendFailure(e)
            }(ThreadUtils.sameThread)
          case None =>
            logWarning("Attempted to request executors before the AM has registered!")
            context.reply(false)
        }

      case k: KillExecutors =>
        amEndpoint match {
          case Some(am) =>
            am.ask[Boolean](k).andThen {
              case Success(b) => context.reply(b)
              case Failure(NonFatal(e)) =>
                logError(s"Sending $k to AM was unsuccessful", e)
                context.sendFailure(e)
            }(ThreadUtils.sameThread)
          case None =>
            logWarning("Attempted to kill executors before the AM has registered!")
            context.reply(false)
        }

      case RetrieveLastAllocatedExecutorId =>
        context.reply(currentExecutorIdCounter)

      case RetrieveDelegationTokens =>
        context.reply(currentDelegationTokens)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      if (amEndpoint.exists(_.address == remoteAddress)) {
        logWarning(s"ApplicationMaster has disassociated: $remoteAddress")
        amEndpoint = None
      }
    }
  }
}

private[spark] object YarnSchedulerBackend {
  val ENDPOINT_NAME = "YarnScheduler"
}
