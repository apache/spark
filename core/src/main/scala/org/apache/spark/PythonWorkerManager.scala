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

import scala.collection.mutable

import org.apache.spark.api.python.{PythonWorker, PythonWorkerFactory}
import org.apache.spark.internal.Logging

/**
 * A key for the [[PythonWorkerFactory]] cache inside [[PythonWorkerManager]].
 * @param pythonExec
 *   The python executable to run the Python worker.
 * @param workerModule
 *   The worker module to be called in the worker, e.g., "pyspark.worker".
 * @param daemonModule
 *   The daemon module name to reuse the worker, e.g., "pyspark.daemon".
 * @param envVars
 *   The environment variables for the worker.
 */
private case class PythonWorkersKey(
    pythonExec: String,
    workerModule: String,
    daemonModule: String,
    envVars: Map[String, String])

/**
 * Manages the lifecycle of [[PythonWorkerFactory]] instances for a Spark environment. Each
 * factory is keyed by its worker configuration and reused across requests with the same
 * configuration.
 */
private[spark] class PythonWorkerManager extends Logging {

  private val pythonWorkers = mutable.HashMap[PythonWorkersKey, PythonWorkerFactory]()

  def createPythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String],
      useDaemon: Boolean): (PythonWorker, Option[ProcessHandle]) = {
    synchronized {
      val key = PythonWorkersKey(pythonExec, workerModule, daemonModule, envVars)
      val workerFactory = pythonWorkers.getOrElseUpdate(
        key,
        new PythonWorkerFactory(pythonExec, workerModule, daemonModule, envVars, useDaemon))
      if (workerFactory.useDaemonEnabled != useDaemon) {
        throw SparkException.internalError("PythonWorkerFactory is already created with " +
          s"useDaemon = ${workerFactory.useDaemonEnabled}, but now is requested with " +
          s"useDaemon = $useDaemon. This is not allowed to change after the PythonWorkerFactory " +
          s"is created given the same key: $key.")
      }
      workerFactory.create()
    }
  }

  def createPythonWorker(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      useDaemon: Boolean): (PythonWorker, Option[ProcessHandle]) = {
    createPythonWorker(
      pythonExec,
      workerModule,
      PythonWorkerFactory.defaultDaemonModule,
      envVars,
      useDaemon)
  }

  def destroyPythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    synchronized {
      val key = PythonWorkersKey(pythonExec, workerModule, daemonModule, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  def destroyPythonWorker(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    destroyPythonWorker(
      pythonExec,
      workerModule,
      PythonWorkerFactory.defaultDaemonModule,
      envVars,
      worker)
  }

  def releasePythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    synchronized {
      val key = PythonWorkersKey(pythonExec, workerModule, daemonModule, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }

  def releasePythonWorker(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    releasePythonWorker(
      pythonExec,
      workerModule,
      PythonWorkerFactory.defaultDaemonModule,
      envVars,
      worker)
  }

  def stop(): Unit = synchronized {
    pythonWorkers.values.foreach(_.stop())
  }
}
