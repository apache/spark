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

package org.apache.spark.deploy

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkConf
import org.apache.spark.api.conda.CondaEnvironment
import org.apache.spark.api.conda.CondaEnvironmentManager
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * A runner template used to launch applications using Conda. It bootstraps a Conda env,
 * then delegates to the [[CondaRunner.run]].
 */
abstract class CondaRunner extends Logging {
  final def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    if (CondaEnvironmentManager.isConfigured(sparkConf)) {
      val condaBootstrapDeps = sparkConf.get(CONDA_BOOTSTRAP_PACKAGES)
      val condaChannelUrls = sparkConf.get(CONDA_CHANNEL_URLS)
      val condaBaseDir = Utils.createTempDir(Utils.getLocalDir(sparkConf), "conda").getAbsolutePath
      val condaEnvironmentManager = CondaEnvironmentManager.fromConf(sparkConf)
      val environment = condaEnvironmentManager
        .create(condaBaseDir, condaBootstrapDeps, condaChannelUrls)

      // Save this as a global in order for SparkContext to be able to access it later, in case we
      // are shelling out, but providing a bridge back into this JVM.
      require(CondaRunner.condaEnvironment.compareAndSet(None, Some(environment)),
        "Couldn't set condaEnvironment to the newly created environment, it was already set to: "
          + CondaRunner.condaEnvironment.get())

      run(args, Some(environment))
    } else {
      run(args, None)
    }
  }

  def run(args: Array[String], maybeConda: Option[CondaEnvironment]): Unit
}

object CondaRunner {
  private[spark] val condaEnvironment: AtomicReference[Option[CondaEnvironment]] =
      new AtomicReference(None)
}
