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

import org.apache.spark.{SparkConf, Logging, SecurityManager}
import org.apache.mesos.{Executor => MesosExecutor, ExecutorDriver, MesosExecutorDriver, MesosNativeLibrary}
import org.apache.spark.util.{Utils, SignalLogger}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.mesos.Protos._
import org.apache.spark.deploy.worker.StandaloneWorkerShuffleService
import scala.collection.JavaConversions._
import scala.io.Source
import java.io.{File, PrintWriter}

/**
 * The Coarse grained Mesos executor backend is responsible for launching the shuffle service
 * and the CoarseGrainedExecutorBackend actor.
 * This is assuming the scheduler detected that the shuffle service is enabled and launches
 * this class instead of CoarseGrainedExecutorBackend directly.
 */
private[spark] class CoarseGrainedMesosExecutorBackend(val sparkConf: SparkConf)
  extends MesosExecutor
  with Logging {

  private var shuffleService: StandaloneWorkerShuffleService = null
  private var driver: ExecutorDriver = null
  private var executorProc: Process = null
  private var taskId: TaskID = null

  override def registered(
      driver: ExecutorDriver,
      executorInfo: ExecutorInfo,
      frameworkInfo: FrameworkInfo,
      slaveInfo: SlaveInfo) {

    this.driver = driver
    logInfo("Coarse Grain Mesos Executor '" + executorInfo.getExecutorId.getValue +
            "' is registered.")

    if (shuffleService == null) {
      sparkConf.set("spark.shuffle.service.enabled", "true")
      shuffleService = new StandaloneWorkerShuffleService(sparkConf, new SecurityManager(sparkConf))
      shuffleService.startIfEnabled()
    }
  }

  override def launchTask(d: ExecutorDriver, taskInfo: TaskInfo) {
    if (executorProc != null) {
      logError("Received LaunchTask while executor is already running")
      val status = TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId)
        .setSlaveId(taskInfo.getSlaveId)
        .setState(TaskState.TASK_FAILED)
        .setMessage("Received LaunchTask while executor is already running")
        .build()
      d.sendStatusUpdate(status)
      return
    }

    // We are launching the CoarseGrainedExecutorBackend via subprocess
    // because the backend is designed to run in its own process.
    // Since it's a shared class we are preserving the existing behavior
    // and launching it as a subprocess here.
    val command = Utils.deserialize[String](taskInfo.getData().toByteArray)

    // Mesos only work on linux platforms, as mesos command executor also
    // executes with /bin/sh -c, we assume this will also work under mesos execution.
    val pb = new ProcessBuilder(List("/bin/sh", "-c", command))

    val currentEnvVars = pb.environment()
    for (variable <- taskInfo.getExecutor.getCommand.getEnvironment.getVariablesList()) {
      currentEnvVars.put(variable.getName, variable.getValue)
    }

    executorProc = pb.start()

    new Thread("stderr reader for task " + taskInfo.getTaskId.getValue) {
      override def run() {
        for (line <- Source.fromInputStream(executorProc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()

    new Thread("stdout reader for task " + taskInfo.getTaskId.getValue) {
      override def run() {
        for (line <- Source.fromInputStream(executorProc.getInputStream).getLines) {
          System.out.println(line)
        }
      }
    }.start()

    new Thread("process waiter for mesos executor for task " + taskInfo.getTaskId.getValue) {
      override def run() {
        executorProc.waitFor()
        val (state, msg) = if (executorProc.exitValue() == 0) {
          (TaskState.TASK_FINISHED, "")
        } else {
          (TaskState.TASK_FAILED, "Exited with status: " + executorProc.exitValue().toString)
        }

        // We leave the shuffle service running after the task.
        cleanup(state, msg)
      }
    }.start()

    taskId = taskInfo.getTaskId
  }

  override def error(d: ExecutorDriver, message: String) {
    logError("Error from Mesos: " + message)
  }

  override def killTask(d: ExecutorDriver, t: TaskID) {
    if (taskId == null) {
      logError("Received killtask when no process is initialized")
      return
    }

    if (!taskId.getValue.equals(t.getValue)) {
      logError("Asked to kill task '" + t.getValue + "' but executor is running task '" +
        taskId.getValue + "'")
      return
    }

    assert(executorProc != null)
    // We only destroy the coarse grained executor but leave the shuffle
    // service running for other tasks that might be reusing this executor.
    // This is no-op if the process already finished.
    executorProc.destroy()
    cleanup(TaskState.TASK_KILLED)
  }

  def cleanup(state: TaskState, msg: String = ""): Unit = synchronized {
    if (driver == null) {
      logError("Cleaning up process but driver is not initialized")
      return
    }

    if (executorProc == null) {
      logDebug("Process is not started or already cleaned up")
      return
    }

    assert(taskId != null)

    driver.sendStatusUpdate(TaskStatus.newBuilder()
      .setState(state)
      .setMessage(msg)
      .setTaskId(taskId)
      .build)

    executorProc = null
    taskId = null
  }

  override def reregistered(d: ExecutorDriver, p2: SlaveInfo) {}

  override def disconnected(d: ExecutorDriver) {}

  override def frameworkMessage(d: ExecutorDriver, data: Array[Byte]) {}

  override def shutdown(d: ExecutorDriver) {
    if (executorProc != null) {
      killTask(d, taskId)
    }

    if (shuffleService != null) {
      shuffleService.stop()
      shuffleService = null
    }
  }
}

private[spark] object CoarseGrainedMesosExecutorBackend extends Logging {
  def main(args: Array[String]) {
    SignalLogger.register(log)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      MesosNativeLibrary.load()
      val sparkConf = new SparkConf()
      // Create a new Executor and start it running
      val runner = new CoarseGrainedMesosExecutorBackend(sparkConf)
      new MesosExecutorDriver(runner).run()
    }
  }
}
