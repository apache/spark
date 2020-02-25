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
package org.apache.spark.internal.config

private[spark] object R {

  val R_BACKEND_CONNECTION_TIMEOUT = ConfigBuilder("spark.r.backendConnectionTimeout")
    .intConf
    .createWithDefault(6000)

  val R_NUM_BACKEND_THREADS = ConfigBuilder("spark.r.numRBackendThreads")
    .intConf
    .createWithDefault(2)

  val R_HEARTBEAT_INTERVAL = ConfigBuilder("spark.r.heartBeatInterval")
    .intConf
    .createWithDefault(100)

  val SPARKR_COMMAND = ConfigBuilder("spark.sparkr.r.command")
    .stringConf
    .createWithDefault("Rscript")

  val R_COMMAND = ConfigBuilder("spark.r.command")
    .stringConf
    .createOptional
}
