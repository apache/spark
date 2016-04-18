/*
 * EAGLE 
 *
 * Copyright 2016 Operating Systems Laboratory EPFL
 *
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

package org.apache.spark.scheduler.eagle

import java.io._
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.scheduler.Pool
import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.storage.BlockManagerId

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import ch.epfl.eagle.api.EagleFrontendClient
import ch.epfl.eagle.thrift.FrontendService
import ch.epfl.eagle.thrift.TFullTaskId
import ch.epfl.eagle.thrift.TPlacementPreference
import ch.epfl.eagle.thrift.TTaskSpec
import ch.epfl.eagle.thrift.TUserGroupInfo

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.internal.Logging

private[spark] class EagleScheduler(val sc: SparkContext,
                                    host: String, port: String, frameworkName: String)
    extends TaskScheduler with FrontendService.Iface with Logging {
  private var backend: SchedulerBackend = null

  val conf = sc.conf
  private val client = new EagleFrontendClient
  private val user = new TUserGroupInfo("sparkUser", "group", 0)
  private val taskId = new AtomicInteger

  // Mapping of task ids to Tasks because we need to pass a Task back to the listener on task end.
  private val tidToTask = new HashMap[String, Task[_]]()
  private val requestsStartedTime = new HashMap[String, Long]()
  private val requestsShortTaskDuration = new HashMap[String, Pair[Boolean, Double]]()

  private var ser = SparkEnv.get.closureSerializer.newInstance()

  // The pool is just used for the UI -- so fine if we don't set it.
  val schedulingMode: SchedulingMode = SchedulingMode.withName("FIFO")
  var rootPool = new Pool("", schedulingMode, 0, 0)

  override def submitTasks(taskSet: TaskSet) = synchronized {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    val eagleTasks = ((1 to tasks.length) zip tasks).map(t => {
      val spec = new TTaskSpec
      spec.setPreference {
        val placement = new TPlacementPreference
        t._2.preferredLocations.foreach(p => placement.addToNodes(p.host))
        placement
      }
      val serializedTask: ByteBuffer = Task.serializeWithDependencies(t._2, sc.addedFiles, sc.addedJars, ser)
      val taskName = s"task ${taskId} in stage ${taskSet.id}"
      val serializedTaskDesc: ByteBuffer = ser.serialize(new TaskDescription(taskId = taskId.longValue(),
        attemptNumber = 1, "dummy_id_exec",
        taskName, t._1, serializedTask))
      spec.setMessage(serializedTaskDesc)
      val tid = taskId.incrementAndGet().toString()
      tidToTask(tid) = t._2
      spec.setTaskId(tid)
      spec
    })

    val description = {
      if (taskSet.properties == null) {
        ""
      } else {
        val sparkJobDescription = taskSet.properties.getProperty(
          SparkContext.SPARK_JOB_DESCRIPTION, "").replace("\n", " ")
        "%s-%s".format(sparkJobDescription, taskSet.stageId)
      }
    }
    
    val shortJob = taskSet.properties.getProperty(EagleProperties.EAGLE_JOB_SHORT, EagleProperties.EAGLE_DEFAULT_JOB_SHORT) //new java.lang.Boolean(true)
    val estimatedDuration = taskSet.properties.getProperty(EagleProperties.EAGLE_JOB_ESTIMATED_RUNTIME, EagleProperties.EAGLE_DEFAULT_JOB_ESTIMATED_RUNTIME) //new java.lang.Double(0.0)
    
    val tasksHead = taskSet.tasks.head
    // [[ PAMELA : trying to get something from RDD
      /*    logInfo("EagleScheduler submitTasks " + taskSet.tasks.head.getClass)
		val (shortJob, estimatedDuration) = tasksHead match {

    rtm_rdd match {
         case mrdd: MappedRDD[_, _] =>
           logInfo("EagleScheduler LIST DEPENDENCIES!!")
           mrdd.getDependencies.foreach { dependency =>  logInfo(" Dependency rdd"+dependency.rdd+" class "+dependency.rdd.getClass)}//" onetoone rdd"+dependency.rdd.asInstanceOf[OneToOneDependency[_]].rdd)}
            val rddDependency = mrdd.getDependencies.head.asInstanceOf[OneToOneDependency[_]].rdd
            rddDependency match {
              case rdd: ParallelCollectionRDD[_]=>
                val headJob = rdd.data.seq            
                logInfo("MappedRdd head of rdd ParallelCollectionRDD data.seq.head class" + headJob.head.getClass() + " print first element only " + headJob.head.toString())
                headJob.head match{
                     case pair: SparkPair[_,SparkPair[Boolean,Double]] =>
                     logInfo("ParallelCollectionRDD data.seq.head firstTask second element " + pair.second + " shortTask " + pair.second.first)
                     (pair.second.first,pair.second.second)                       
                }                                
              case notSupported => // rddDependency types not supported
                logInfo("Not ParallelCollectionRDD, not cased " + notSupported.getClass+" going for default values")
                 (defaultShortJob, defaultEstimatedDuration)
            }
          case notSupported => // original RDD not supported
            logInfo("Not MappedRDD, not cased " + notSupported.getClass+" going for default values")
            (defaultShortJob, defaultEstimatedDuration)
         }  */

    val startTime = System.nanoTime
    requestsStartedTime(taskId.get().toString()) = startTime
    logInfo("Request from stage " + tasksHead.stageId + " taskid " + taskId + " start elapsed time: " + startTime + " shortJob " + shortJob + " estimatedDuration " + estimatedDuration)
    requestsStartedTime.foreach(st => logDebug("RequestsStartedTime " + st.toString()))

    new Thread(new Runnable() {
      override def run() {
        client.submitJob(frameworkName, eagleTasks.toList, new java.lang.Boolean(shortJob), new java.lang.Double(estimatedDuration), user, description)
        logInfo("Submitted taskSet with id=%s time=%s".format(taskSet.id, System.currentTimeMillis))
      }
    }).start()
  }

  def initialize(backend: SchedulerBackend) {
    this.backend = backend;
  }

  override def start() {
    backend.start();
    val socketAddress = new InetSocketAddress(host, port.toInt)
    logInfo("EagleScheduler start call %s %s %s".format(host, port.toInt, frameworkName))
    client.initialize(socketAddress, frameworkName, this)
  }

  override def stop() {
    backend.stop();
    // Do nothing.
  }

  override def defaultParallelism() = System.getProperty("spark.default.parallelism", "8").toInt
  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  // TODO: have taskStarted message from Sparrow; on receiving that call taskStarted
  // on task listener.

  override def frontendMessage(taskId: TFullTaskId, statusInt: Int, message: ByteBuffer) = synchronized {
    logInfo(s"got frontend msg taskId $taskId, $statusInt")
    TaskState.apply(statusInt) match {
      case TaskState.FINISHED =>
        val result = ser.deserialize[DirectTaskResult[_]](message, getClass.getClassLoader)
        // FIXME: get this information from Sparrow, rather than fudging it.
        val taskInfo = new TaskInfo(
          taskId.getTaskId.toLong,
          0,
          0,
          System.currentTimeMillis,
          "dummyexecId",
          "foo:bar",
          TaskLocality.PROCESS_LOCAL, conf.getBoolean("spark.speculation", false))
        taskInfo.finishTime = System.currentTimeMillis
        taskInfo.failed = false
        dagScheduler.taskEnded(
          tidToTask(taskId.getTaskId()),
          Success,
          result.value,
          result.accumUpdates,
          taskInfo)

        if (requestsStartedTime.contains(taskId.getRequestId())) {
          val startedTime = requestsStartedTime(taskId.getRequestId())
          val finishedTime = System.nanoTime()
          logInfo("Finished request from requestid " + taskId.getRequestId() + " taskid " + taskId.getTaskId + " finish elapsed time: " + finishedTime)
          logInfo("Total time from submission to finishing stage in milliseconds " + ((finishedTime - startedTime) / 1000000))
        } else {
          logDebug("Finished request NOT FOUND requestid " + taskId.getRequestId() + " taskid " + taskId.getTaskId)
          requestsStartedTime.foreach(st => logDebug("RequestsStartedTime " + st.toString()))
        }

      case status =>
        // TODO: the fact that we don't support task started right now causes UI problems.
        logWarning("Got unexpected task state: " + status)
    }
  }

  // Cancel a stage.
  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {
    // We don't support this atm.
  }

  /**
   * Get an application's attempt ID associated with the job.
   *
   * @return An application's Attempt ID
   */
  override def applicationAttemptId(): Option[String] = {
    Some("fake-attempt-ID") // FIXME: get real ID here.
  }

  /**
   * Process a lost executor
   */
  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    // noop. //fixme
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
    execId: String,
    accumUpdates: Array[(Long, Seq[AccumulableInfo])],
    blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
      accumUpdates.flatMap {
        case (id, updates) =>
          Seq((id, -1, -1, updates)) // FIXME Fudged values.
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
  }

}
