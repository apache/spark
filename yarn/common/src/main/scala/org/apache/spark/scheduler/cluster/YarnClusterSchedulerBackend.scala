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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.deploy.yarn.ApplicationMasterArguments
import org.apache.spark.scheduler.TaskSchedulerImpl

private[spark] class YarnClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
  with Logging {

  private def addArg(optionName: String, envVar: String, sysProp: String,
      arrayBuf: ArrayBuffer[String]) {
    if (System.getenv(envVar) != null) {
      arrayBuf += (optionName, System.getenv(envVar))
    } else if (sc.getConf.contains(sysProp)) {
      arrayBuf += (optionName, sc.getConf.get(sysProp))
    }
  }

  override def start() {
    super.start()
    val argsArrayBuf = new ArrayBuffer[String]()
    List(("--num-executors", "SPARK_EXECUTOR_INSTANCES", "spark.executor.instances"),
      ("--num-executors", "SPARK_WORKER_INSTANCES", "spark.worker.instances"))
      .foreach { case (optName, envVar, sysProp) => addArg(optName, envVar, sysProp, argsArrayBuf) }
    val args = new ApplicationMasterArguments(argsArrayBuf.toArray)
    totalExecutors.set(args.numExecutors)
  }
}
