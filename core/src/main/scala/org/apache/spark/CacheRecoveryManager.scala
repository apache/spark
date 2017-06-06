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

package org.apache.spark

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

import com.google.common.cache.CacheBuilder

import org.apache.spark.CacheRecoveryManager.{DoneRecovering, KillReason, Timeout}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.DYN_ALLOCATION_CACHE_RECOVERY_TIMEOUT
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.ThreadUtils

/**
 * Responsible for asynchronously replicating all of an executor's cached blocks, and then shutting
 * it down.
 */
private class CacheRecoveryManager(
    blockManagerMasterEndpoint: RpcEndpointRef,
    executorAllocationManager: ExecutorAllocationManager,
    conf: SparkConf)
  extends Logging {

  private val forceKillAfterS = conf.get(DYN_ALLOCATION_CACHE_RECOVERY_TIMEOUT)
  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("cache-recovery-manager-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(threadPool)
  private val scheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("cache-recovery-shutdown-timers")
  private val recoveringExecutors = CacheBuilder.newBuilder()
    .expireAfterWrite(forceKillAfterS * 2, TimeUnit.SECONDS)
    .build[String, String]()

  /**
   * Start the recover cache shutdown process for these executors
   *
   * @param execIds the executors to start shutting down
   * @return a sequence of futures of Unit that will complete once the executor has been killed.
   */
  def startCacheRecovery(execIds: Seq[String]): Future[Seq[KillReason]] = {
    logDebug(s"Recover cached data before shutting down executors ${execIds.mkString(", ")}.")
    val canBeRecovered: Future[Seq[String]] = checkMem(execIds)

    canBeRecovered.flatMap { execIds =>
      execIds.foreach { execId => recoveringExecutors.put(execId, execId) }
      Future.sequence(execIds.map { replicateUntilTimeoutThenKill })
    }
  }

  def replicateUntilTimeoutThenKill(execId: String): Future[KillReason] = {
    val timeoutFuture = returnAfterTimeout(Timeout, forceKillAfterS)
    val replicationFuture = replicateUntilDone(execId)

    Future.firstCompletedOf(List(timeoutFuture, replicationFuture)).andThen {
      case scala.util.Success(DoneRecovering) =>
        logTrace(s"Done recovering blocks on $execId, killing now")
      case scala.util.Success(Timeout) =>
        logWarning(s"Couldn't recover cache on $execId before $forceKillAfterS second timeout")
      case Failure(ex) =>
        logWarning(s"Error recovering cache on $execId", ex)
    }.andThen {
      case _ =>
        kill(execId)
    }
  }

  /**
   * Given a list of executors that will be shut down, check if there is enough free memory on the
   * rest of the cluster to hold their data. Return a list of just the executors for which there
   * will be enough space. Executors are included smallest first.
   *
   * This is a best guess implementation and it is not guaranteed that all returned executors
   * will succeed. For example a block might be too big to fit on any one specific executor.
   *
   * @param execIds executors which will be shut down
   * @return a Seq of the executors we do have room for
   */
  private def checkMem(execIds: Seq[String]): Future[Seq[String]] = {
    val execsToShutDown = execIds.toSet
    // Memory Status is a map of executor Id to a tuple of Max Memory and remaining memory on that
    // executor.
    val futureMemStatusByBlockManager =
        blockManagerMasterEndpoint.ask[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)

    val futureMemStatusByExecutor = futureMemStatusByBlockManager.map { memStat =>
      memStat.map { case (blockManagerId, mem) => blockManagerId.executorId -> mem }
    }

    futureMemStatusByExecutor.map { memStatusByExecutor =>
      val (expiringMemStatus, remainingMemStatus) = memStatusByExecutor.partition {
        case (execId, _) => execsToShutDown.contains(execId)
      }
      val freeMemOnRemaining = remainingMemStatus.values.map(_._2).sum

      // The used mem on each executor sorted from least used mem to greatest
      val executorAndUsedMem: Seq[(String, Long)] =
        expiringMemStatus.map { case (execId, (maxMem, remainingMem)) =>
          val usedMem = maxMem - remainingMem
          execId -> usedMem
        }.toSeq.sortBy { case (_, usedMem) => usedMem }

      executorAndUsedMem
        .scan(("start", freeMemOnRemaining)) {
          case ((_, freeMem), (execId, usedMem)) => (execId, freeMem - usedMem)
        }
        .drop(1)
        .filter { case (_, freeMem) => freeMem > 0 }
        .map(_._1)
    }
  }

  /**
   * Given a value and a timeout in seconds, complete the future with the value when time is up.
   *
   * @param value The value to be returned after timeout period
   * @param seconds the number of seconds to wait
   * @return a future that will hold the value given after a timeout
   */
  private def returnAfterTimeout[T](value: T, seconds: Long): Future[T] = {
    val p = Promise[T]()
    val runnable = new Runnable {
      def run(): Unit = { p.success(value) }
    }
    scheduler.schedule(runnable, seconds, TimeUnit.SECONDS)
    p.future
  }

  /**
   * Recover cached RDD blocks off of an executor until there are no more, or until
   * there is an error
   *
   * @param execId the id of the executor to be killed
   * @return a Future of Unit that will complete once all blocks have been replicated.
   */
  private def replicateUntilDone(execId: String): Future[KillReason] = {
    recoverLatestBlock(execId).flatMap { moreBlocks =>
      if (moreBlocks) replicateUntilDone(execId) else Future.successful(DoneRecovering)
    }
  }

  /**
   * Ask the BlockManagerMaster to replicate the latest cached rdd block off of this executor on to
   * a surviving executor, and then remove the block from this executor
   *
   * @param execId the executor to recover a block from
   * @return A future that will hold true if a block was recovered, false otherwise.
   */
  private def recoverLatestBlock(execId: String): Future[Boolean] = {
    blockManagerMasterEndpoint
      .ask[Boolean](RecoverLatestRDDBlock(execId, recoveringExecutors.asMap.keySet.asScala.toSeq))
  }

  /**
   * Remove the executor from the list of currently recovering executors and then kill it.
   *
   * @param execId the id of the executor to be killed
   */
  private def kill(execId: String): Unit = {
    executorAllocationManager.killExecutors(Seq(execId))
  }

  /**
   * Stops all thread pools
   */
  def stop(): Unit = {
    threadPool.shutdownNow()
    scheduler.shutdownNow()
  }
}

private object CacheRecoveryManager {
  def apply(eam: ExecutorAllocationManager, conf: SparkConf): CacheRecoveryManager = {
    val bmme = SparkEnv.get.blockManager.master.driverEndpoint
    new CacheRecoveryManager(bmme, eam, conf)
  }

  sealed trait KillReason
  case object Timeout extends KillReason
  case object DoneRecovering extends KillReason
}
