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

package org.apache.spark.executor

import java.nio.ByteBuffer

import scala.collection.JavaConversions._

import org.apache.mesos.protobuf.ByteString
import org.apache.mesos.{Executor => MesosExecutor, ExecutorDriver, MesosExecutorDriver, MesosNativeLibrary}
import org.apache.mesos.Protos.{TaskStatus => MesosTaskStatus, _}

import org.apache.spark.{Logging, TaskState, SparkConf, SparkEnv}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{SignalLogger, Utils}

private[spark] class MesosExecutorBackend
  extends MesosExecutor
  with ExecutorBackend
  with Logging {

  var executor: Executor = null
  var driver: ExecutorDriver = null

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    val mesosTaskId = TaskID.newBuilder().setValue(taskId.toString).build()
    driver.sendStatusUpdate(MesosTaskStatus.newBuilder()
      .setTaskId(mesosTaskId)
      .setState(TaskState.toMesos(state))
      .setData(ByteString.copyFrom(data))
      .build())
  }

  override def registered(
      driver: ExecutorDriver,
      executorInfo: ExecutorInfo,
      frameworkInfo: FrameworkInfo,
      slaveInfo: SlaveInfo) {

    // Get num cores for this task from ExecutorInfo, created in MesosSchedulerBackend.
    val cpusPerTask = executorInfo.getResourcesList
      .find(_.getName == "cpus")
      .map(_.getScalar.getValue.toInt)
      .getOrElse(0)
    val executorId = executorInfo.getExecutorId.getValue

    logInfo(s"Registered with Mesos as executor ID $executorId with $cpusPerTask cpus")
    this.driver = driver
    val properties = Utils.deserialize[Array[(String, String)]](executorInfo.getData.toByteArray) ++
      Seq[(String, String)](("spark.app.id", frameworkInfo.getId.getValue))
    val conf = new SparkConf(loadDefaults = true).setAll(properties)
    val port = conf.getInt("spark.executor.port", 0)
    val env = SparkEnv.createExecutorEnv(
      conf, executorId, slaveInfo.getHostname, port, cpusPerTask, isLocal = false)

    executor = new Executor(
      executorId,
      slaveInfo.getHostname,
      env)
  }

  override def launchTask(d: ExecutorDriver, taskInfo: TaskInfo) {
    val taskId = taskInfo.getTaskId.getValue.toLong
    if (executor == null) {
      logError("Received launchTask but executor was null")
    } else {
      executor.launchTask(this, taskId, taskInfo.getName, taskInfo.getData.asReadOnlyByteBuffer)
    }
  }

  override def error(d: ExecutorDriver, message: String) {
    logError("Error from Mesos: " + message)
  }

  override def killTask(d: ExecutorDriver, t: TaskID) {
    if (executor == null) {
      logError("Received KillTask but executor was null")
    } else {
      // TODO: Determine the 'interruptOnCancel' property set for the given job.
      executor.killTask(t.getValue.toLong, interruptThread = false)
    }
  }

  override def reregistered(d: ExecutorDriver, p2: SlaveInfo) {}

  override def disconnected(d: ExecutorDriver) {}

  override def frameworkMessage(d: ExecutorDriver, data: Array[Byte]) {}

  override def shutdown(d: ExecutorDriver) {}
}

/**
 * Entry point for Mesos executor.
 */
private[spark] object MesosExecutorBackend extends Logging {
  def main(args: Array[String]) {
    SignalLogger.register(log)
    SparkHadoopUtil.get.runAsSparkUser { () =>
        MesosNativeLibrary.load()
        // Create a new Executor and start it running
        val runner = new MesosExecutorBackend()
        new MesosExecutorDriver(runner).run()
    }
  }
}
