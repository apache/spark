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

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.api.python.{PythonWorker, PythonWorkerFactory}

/**
 * The arguments for the PythonWorker.
 * @param pythonExec The python executable to run the Python worker.
 * @param workerModule The worker module to be called in the worker, e.g., "pyspark.worker".
 * @param daemonModule The daemon module name to reuse the worker, e.g., "pyspark.daemon".
 * @param envVars The environment variables for the worker.
 * @param useDaemon Whether to use the daemon.
 */
private[spark] case class PythonWorkerArgs(
  pythonExec: String,
  workerModule: String,
  envVars: Map[String, String],
  useDaemon: Boolean,
  daemonModule: String
)

/**
 * The PythonWorkerManager is responsible for creating, releasing, and destroying Python workers.
 * It maps each unique set of PythonWorkerArgs to its own PythonWorkerFactory. The python worker
 * args can not be changed after a worker was created, thus we need to ensure that all workers
 * within a factory have the same args.
 */
class PythonWorkerManager(val conf: SparkConf) { self =>
  // visible for testing
  @GuardedBy("self")
  private[spark] val workerFactories = mutable.HashMap[PythonWorkerArgs, PythonWorkerFactory]()

  /**
   * Creates a new Python worker. Creates a new PythonWorkerFactory if it doesn't exist.
   *
   * @param args
   * @return
   */
  private[spark] def createPythonWorker(args: PythonWorkerArgs): PythonWorker = {
    self.synchronized {
      workerFactories.getOrElseUpdate(args, new PythonWorkerFactory(args))
        .create()
    }
  }

  /**
   * Releases the Python worker, so it can be reused. The factory might keep the
   * worker alive in the idle pool.
   *
   * @param worker
   */
  private[spark] def releasePythonWorker(worker: PythonWorker): Unit = {
    synchronized {
      workerFactories.get(worker.args).foreach(_.releaseWorker(worker))
    }
  }

  /**
   * Destroys the Python worker, terminates it.
   *
   * @param worker
   * @param exception
   */
  private[spark] def destroyPythonWorker(
    worker: PythonWorker, exception: Option[Throwable]): Unit = {
    synchronized {
      workerFactories.get(worker.args).foreach(_.stopWorker(worker, exception))
    }
  }

  private[spark] def stop(): Unit = {
    workerFactories.values.foreach(_.stop())
  }
}
