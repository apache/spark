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

import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.ApplicationMasterArguments
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.IntParam

private[spark] class YarnClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem) {

  if (conf.getOption("spark.scheduler.minRegisteredExecutorsRatio").isEmpty) {
    minRegisteredRatio = 0.8
    ready = false
  }

  override def start() {
    super.start()
    var numExecutors = ApplicationMasterArguments.DEFAULT_NUMBER_EXECUTORS
    if (System.getenv("SPARK_EXECUTOR_INSTANCES") != null) {
      numExecutors = IntParam.unapply(System.getenv("SPARK_EXECUTOR_INSTANCES")).getOrElse(numExecutors)
    }
    // System property can override environment variable.
    numExecutors = sc.getConf.getInt("spark.executor.instances", numExecutors)
    totalExpectedExecutors.set(numExecutors)
  }
}
