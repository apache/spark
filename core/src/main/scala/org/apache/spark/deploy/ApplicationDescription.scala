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

import java.net.URI

import org.apache.spark.SparkException

private[spark] case class ApplicationDescription(
    name: String,
    maxCores: Option[Int],
    memoryPerExecutorMB: Int,
    command: Command,
    appUiUrl: String,
    eventLogDir: Option[URI] = None,
    // short name of compression codec used when writing event logs, if any (e.g. lzf)
    eventLogCodec: Option[String] = None,
    coresPerExecutor: Option[Int] = None,
    // number of executors this application wants to start with,
    // only used if dynamic allocation is enabled
    initialExecutorLimit: Option[Int] = None,
    user: String = System.getProperty("user.name", "<unknown>"),
    val coresPerTask: Int = 1) {

  // We only check whether coresPerExecutor is no less than coresPerTask when the user has the
  // explicit configuration on this. We have the explicit mechanism to prevent to start executors
  // which cannot execute any task in Master. (SPARK-5337)
  if (coresPerExecutor.getOrElse(Int.MaxValue) < coresPerTask) {
    throw new SparkException(s"illegal configuration for application $name, " +
      s"coresPerExecutor (configured value: ${coresPerExecutor.getOrElse(1)}) cannot be less " +
      s"than coresPerTask (configured value: $coresPerTask)")
  }

  override def toString: String = "ApplicationDescription(" + name + ")"
}
