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
      val condaEnvDir = Utils.createTempDir(Utils.getLocalDir(sparkConf), "conda").getAbsolutePath
      val condaEnvironmentManager = CondaEnvironmentManager.fromConf(sparkConf)
      val environment = condaEnvironmentManager.create(condaEnvDir, condaBootstrapDeps)

      sys.props += CondaEnvironment.CONDA_ENVIRONMENT_PREFIX -> condaEnvDir

      run(args, Some(environment))
    } else {
      run(args, None)
    }
  }

  def run(args: Array[String], maybeConda: Option[CondaEnvironment]): Unit
}
