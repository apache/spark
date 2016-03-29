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

package org.apache.spark.internal

import org.apache.spark.launcher.SparkLauncher

package object config {

  private[spark] val DRIVER_CLASS_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_CLASSPATH).stringConf.optional

  private[spark] val DRIVER_JAVA_OPTIONS =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS).stringConf.optional

  private[spark] val DRIVER_LIBRARY_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH).stringConf.optional

  private[spark] val DRIVER_USER_CLASS_PATH_FIRST =
    ConfigBuilder("spark.driver.userClassPathFirst").booleanConf.withDefault(false)

  private[spark] val EXECUTOR_CLASS_PATH =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH).stringConf.optional

  private[spark] val EXECUTOR_JAVA_OPTIONS =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS).stringConf.optional

  private[spark] val EXECUTOR_LIBRARY_PATH =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_LIBRARY_PATH).stringConf.optional

  private[spark] val EXECUTOR_USER_CLASS_PATH_FIRST =
    ConfigBuilder("spark.executor.userClassPathFirst").booleanConf.withDefault(false)

  private[spark] val IS_PYTHON_APP = ConfigBuilder("spark.yarn.isPython").internal
    .booleanConf.withDefault(false)

  private[spark] val CPUS_PER_TASK = ConfigBuilder("spark.task.cpus").intConf.withDefault(1)

  private[spark] val DYN_ALLOCATION_MIN_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.minExecutors").intConf.withDefault(0)

  private[spark] val DYN_ALLOCATION_INITIAL_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.initialExecutors")
      .fallbackConf(DYN_ALLOCATION_MIN_EXECUTORS)

  private[spark] val DYN_ALLOCATION_MAX_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.maxExecutors").intConf.withDefault(Int.MaxValue)

  private[spark] val SHUFFLE_SERVICE_ENABLED =
    ConfigBuilder("spark.shuffle.service.enabled").booleanConf.withDefault(false)

  private[spark] val KEYTAB = ConfigBuilder("spark.yarn.keytab")
    .doc("Location of user's keytab.")
    .stringConf.optional

  private[spark] val PRINCIPAL = ConfigBuilder("spark.yarn.principal")
    .doc("Name of the Kerberos principal.")
    .stringConf.optional

  private[spark] val EXECUTOR_INSTANCES = ConfigBuilder("spark.executor.instances").intConf.optional

}
