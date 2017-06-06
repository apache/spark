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

package org.apache.spark.scheduler.cluster.nomad

import com.hashicorp.nomad.apimodel.Task

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

private[spark] object ShuffleServiceTask
  extends SparkNomadTaskType("shuffle", "shuffle service",
    ConfigBuilder("spark.nomad.shuffle.memory")
      .doc("The amount of memory that Nomad should allocate for the shuffle service tasks")
      .bytesConf(ByteUnit.MiB)
      .createWithDefaultString("256m")
  ) {

  private val port = ConfigurablePort("shuffleService")

  def configure(
      jobConf: SparkNomadJob.CommonConf,
      conf: SparkConf,
      task: Task,
      reconfiguring: Boolean
  ): String = {
    super.configure(jobConf, conf, task, Seq(port), "spark-class")

    if (!reconfiguring) {
      task
        .addEnv("SPARK_SHUFFLE_OPTS", s"-Dspark.shuffle.service.port=$port")
        .addEnv("SPARK_DAEMON_MEMORY", jvmMemory(conf, task))
        .addEnv("SPARK_LOCAL_DIRS", "${NOMAD_ALLOC_DIR}")

      appendArguments(task, Seq("org.apache.spark.deploy.ExternalShuffleService", "1"))
    }

    port.placeholderInSiblingTasks(task)
  }

}
