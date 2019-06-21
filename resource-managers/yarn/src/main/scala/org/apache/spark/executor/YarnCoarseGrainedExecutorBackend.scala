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

import java.net.URL

import org.apache.spark.SparkEnv
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.YarnContainerInfoHelper

/**
 * Custom implementation of CoarseGrainedExecutorBackend for YARN resource manager.
 * This class extracts executor log URLs and executor attributes from system environment which
 * properties are available for container being set via YARN.
 */
private[spark] class YarnCoarseGrainedExecutorBackend(
    rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostname: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv,
    resourcesFile: Option[String])
  extends CoarseGrainedExecutorBackend(
    rpcEnv,
    driverUrl,
    executorId,
    hostname,
    cores,
    userClassPath,
    env,
    resourcesFile) with Logging {

  private lazy val hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(env.conf)

  override def extractLogUrls: Map[String, String] = {
    YarnContainerInfoHelper.getLogUrls(hadoopConfiguration, container = None)
      .getOrElse(Map())
  }

  override def extractAttributes: Map[String, String] = {
    YarnContainerInfoHelper.getAttributes(hadoopConfiguration, container = None)
      .getOrElse(Map())
  }
}

private[spark] object YarnCoarseGrainedExecutorBackend extends Logging {

  def main(args: Array[String]): Unit = {
    val createFn: (RpcEnv, CoarseGrainedExecutorBackend.Arguments, SparkEnv) =>
      CoarseGrainedExecutorBackend = { case (rpcEnv, arguments, env) =>
      new YarnCoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
        arguments.hostname, arguments.cores, arguments.userClassPath, env,
        arguments.resourcesFileOpt)
    }
    val backendArgs = CoarseGrainedExecutorBackend.parseArguments(args,
      this.getClass.getCanonicalName.stripSuffix("$"))
    CoarseGrainedExecutorBackend.run(backendArgs, createFn)
    System.exit(0)
  }

}
