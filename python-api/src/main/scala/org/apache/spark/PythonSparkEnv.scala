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

import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a thread-local variable, so each thread that accesses these
 * objects needs to have the right SparkEnv set. You can get the current environment with
 * SparkEnv.get (e.g. after creating a SparkContext) and set it with SparkEnv.set.
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 * in a future release.
 */
@DeveloperApi
class PythonSparkEnv(val sparkEnv: SparkEnv) {
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  sparkEnv.closeables += new java.io.Closeable {
    override def close() {
      pythonWorkers.foreach {
        case (key, worker) => worker.stop()
      }
    }
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String]) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers(key).stop()
    }
  }

}

object PythonSparkEnv extends Logging {
  private val env = new ThreadLocal[PythonSparkEnv]

  def get: PythonSparkEnv = {
    if (env.get == null) {
      env.set(new PythonSparkEnv(SparkEnv.get))
    }
    env.get
  }

  def set(e: PythonSparkEnv) {
    env.set(e)
  }

}