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
import org.apache.spark.internal.config

/**
 * Which environment variables of the submitting process reach a driver launched in standalone
 * cluster mode. Both submission gateways use this: the REST client always applies
 * [[filterSystemEnvironment]], and the legacy RPC [[Client]] goes through [[forSubmission]] so
 * `spark.standalone.submit.filterEnvironment` can turn the filtering off.
 */
private[deploy] object DriverEnvironment {

  // SPARK_LOCAL_IP and SPARK_LOCAL_HOSTNAME describe the submitting host rather than the
  // worker the driver runs on, so they are never forwarded to the driver (SPARK-20025).
  val HOST_SPECIFIC_ENV_VARS = Set("SPARK_LOCAL_IP", "SPARK_LOCAL_HOSTNAME")

  // SPARK_HOME and SPARK_CONF_DIR are filtered out because they are usually wrong
  // on the remote machine (SPARK-12345) (SPARK-25934).
  private val EXCLUDED_SPARK_ENV_VARS =
    Set("SPARK_ENV_LOADED", "SPARK_HOME", "SPARK_CONF_DIR") ++ HOST_SPECIFIC_ENV_VARS

  /**
   * Filter non-spark environment variables from any environment.
   */
  def filterSystemEnvironment(env: Map[String, String]): Map[String, String] = {
    env.filter { case (k, _) =>
      k.startsWith("SPARK_") && !EXCLUDED_SPARK_ENV_VARS.contains(k)
    }
  }

  /**
   * Environment variables to forward to the driver. Only variables whose name starts with
   * `SPARK_` are forwarded, excluding `SPARK_ENV_LOADED`, `SPARK_HOME`, `SPARK_CONF_DIR`,
   * `SPARK_LOCAL_IP` and `SPARK_LOCAL_HOSTNAME`. If
   * `spark.standalone.submit.filterEnvironment` is disabled, the full environment of the
   * submitting process is forwarded instead, except `SPARK_LOCAL_IP` and
   * `SPARK_LOCAL_HOSTNAME` (SPARK-20025).
   */
  def forSubmission(conf: SparkConf, env: Map[String, String]): Map[String, String] = {
    if (conf.get(config.STANDALONE_SUBMIT_FILTER_ENVIRONMENT)) {
      filterSystemEnvironment(env)
    } else {
      env -- HOST_SPECIFIC_ENV_VARS
    }
  }
}
