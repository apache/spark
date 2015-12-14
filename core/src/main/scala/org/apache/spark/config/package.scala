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

import org.apache.spark.launcher.SparkLauncher

package object config {
  import ConfigEntry._

  private[spark] val DRIVER_CLASS_PATH = stringConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH)
    .optional

  private[spark] val DRIVER_JAVA_OPTIONS = stringConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)
    .optional

  private[spark] val DRIVER_LIBRARY_PATH = stringConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH)
    .optional

  private[spark] val DRIVER_USER_CLASS_PATH_FIRST = booleanConf("spark.driver.userClassPathFirst",
    defaultValue = Some(false))

  private[spark] val EXECUTOR_CLASS_PATH = stringConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH)
    .optional

  private[spark] val EXECUTOR_JAVA_OPTIONS = stringConf(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS)
    .optional

  private[spark] val EXECUTOR_LIBRARY_PATH = stringConf(SparkLauncher.EXECUTOR_EXTRA_LIBRARY_PATH)
    .optional

  private[spark] val EXECUTOR_USER_CLASS_PATH_FIRST =
    booleanConf("spark.executor.userClassPathFirst", defaultValue = Some(false))

  private[spark] val IS_PYTHON_APP = booleanConf("spark.yarn.isPython",
    defaultValue = Some(false),
    isPublic = false)

  private[spark] val CPUS_PER_TASK = intConf("spark.task.cpus",
    defaultValue = Some(1))

  private[spark] val DYN_ALLOCATION_MIN_EXECUTORS = intConf("spark.dynamicAllocation.minExecutors",
    defaultValue = Some(0))

  private[spark] val DYN_ALLOCATION_INITIAL_EXECUTORS =
    fallbackConf("spark.dynamicAllocation.initialExecutors",
      fallback = DYN_ALLOCATION_MIN_EXECUTORS)

  private[spark] val DYN_ALLOCATION_MAX_EXECUTORS = intConf("spark.dynamicAllocation.maxExecutors",
    defaultValue = Some(Int.MaxValue))

  private[spark] val SHUFFLE_SERVICE_ENABLED = booleanConf("spark.shuffle.service.enabled",
    defaultValue = Some(false))

  private[spark] val KEYTAB = stringConf("spark.yarn.keytab",
    doc = "Location of user's keytab.")
    .optional

  private[spark] val PRINCIPAL = stringConf("spark.yarn.principal",
    doc = "Name of the Kerberos principal.")
    .optional

  private[spark] val EXECUTOR_INSTANCES = intConf("spark.executor.instances").optional

}
