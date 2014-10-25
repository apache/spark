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

import scala.collection.Map
import scala.collection.JavaConversions._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SchedulingMode, Schedulable}
import org.apache.spark.storage.{StorageStatus, StorageUtils, RDDInfo}

/**
 * Trait that implements Spark's status APIs.  This trait is designed to be mixed into
 * SparkContext; it allows the status API code to live in its own file.
 */
private[spark] trait SparkStatusAPI { this: SparkContext =>

  /**
   * Return a map from the slave to the max memory available for caching and the remaining
   * memory available for caching.
   */
  def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
    env.blockManager.master.getMemoryStatus.map { case(blockManagerId, mem) =>
      (blockManagerId.host + ":" + blockManagerId.port, mem)
    }
  }

  /**
   * :: DeveloperApi ::
   * Return information about what RDDs are cached, if they are in mem or on disk, how much space
   * they take, etc.
   */
  @DeveloperApi
  def getRDDStorageInfo: Array[RDDInfo] = {
    val rddInfos = persistentRdds.values.map(RDDInfo.fromRdd).toArray
    StorageUtils.updateRddInfo(rddInfos, getExecutorStorageStatus)
    rddInfos.filter(_.isCached)
  }

  /**
   * Returns an immutable map of RDDs that have marked themselves as persistent via cache() call.
   * Note that this does not necessarily mean the caching or computation was successful.
   */
  def getPersistentRDDs: Map[Int, RDD[_]] = persistentRdds.toMap

  /**
   * :: DeveloperApi ::
   * Return information about blocks stored in all of the slaves
   */
  @DeveloperApi
  def getExecutorStorageStatus: Array[StorageStatus] = {
    env.blockManager.master.getStorageStatus
  }

  /**
   * :: DeveloperApi ::
   * Return pools for fair scheduler
   */
  @DeveloperApi
  def getAllPools: Seq[Schedulable] = {
    // TODO(xiajunluan): We should take nested pools into account
    taskScheduler.rootPool.schedulableQueue.toSeq
  }

  /**
   * :: DeveloperApi ::
   * Return the pool associated with the given name, if one exists
   */
  @DeveloperApi
  def getPoolForName(pool: String): Option[Schedulable] = {
    Option(taskScheduler.rootPool.schedulableNameToSchedulable.get(pool))
  }

  /**
   * Return current scheduling mode
   */
  def getSchedulingMode: SchedulingMode.SchedulingMode = {
    taskScheduler.schedulingMode
  }


  /**
   * Return a list of all known jobs in a particular job group.  The returned list may contain
   * running, failed, and completed jobs, and may vary across invocations of this method.  This
   * method does not guarantee the order of the elements in its result.
   */
  def getJobIdsForGroup(jobGroup: String): Array[Int] = {
    jobProgressListener.synchronized {
      val jobData = jobProgressListener.jobIdToData.valuesIterator
      jobData.filter(_.jobGroup.exists(_ == jobGroup)).map(_.jobId).toArray
    }
  }

  /**
   * Returns job information, or `None` if the job info could not be found or was garbage collected.
   */
  def getJobInfo(jobId: Int): Option[SparkJobInfo] = {
    jobProgressListener.synchronized {
      jobProgressListener.jobIdToData.get(jobId).map { data =>
        new SparkJobInfoImpl(jobId, data.stageIds.toArray, data.status)
      }
    }
  }

  /**
   * Returns stage information, or `None` if the stage info could not be found or was
   * garbage collected.
   */
  def getStageInfo(stageId: Int): Option[SparkStageInfo] = {
    jobProgressListener.synchronized {
      for (
        info <- jobProgressListener.stageIdToInfo.get(stageId);
        data <- jobProgressListener.stageIdToData.get((stageId, info.attemptId))
      ) yield {
        new SparkStageInfoImpl(
          stageId,
          info.attemptId,
          info.name,
          info.numTasks,
          data.numActiveTasks,
          data.numCompleteTasks,
          data.numFailedTasks)
      }
    }
  }
}
