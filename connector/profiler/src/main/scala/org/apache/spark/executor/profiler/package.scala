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

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder

package object profiler {

  private[profiler] val EXECUTOR_PROFILING_ENABLED =
    ConfigBuilder("spark.executor.profiling.enabled")
      .doc("Turn on code profiling via async_profiler in executors.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  private[profiler] val EXECUTOR_PROFILING_DFS_DIR =
    ConfigBuilder("spark.executor.profiling.dfsDir")
      .doc("HDFS compatible file-system path to where the profiler will write output jfr files.")
      .version("4.0.0")
      .stringConf
      .createOptional

  private[profiler] val EXECUTOR_PROFILING_LOCAL_DIR =
    ConfigBuilder("spark.executor.profiling.localDir")
      .doc("Local file system path on executor where profiler output is saved. Defaults to the " +
        "working directory of the executor process.")
      .version("4.0.0")
      .stringConf
      .createWithDefault(".")

  private[profiler] val EXECUTOR_PROFILING_OPTIONS =
    ConfigBuilder("spark.executor.profiling.options")
      .doc("Options to pass on to the async profiler.")
      .version("4.0.0")
      .stringConf
      .createWithDefault("event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s")

  private[profiler] val EXECUTOR_PROFILING_FRACTION =
    ConfigBuilder("spark.executor.profiling.fraction")
      .doc("Fraction of executors to profile")
      .version("4.0.0")
      .doubleConf
      .checkValue(v => v >= 0.0 && v <= 1.0,
        "Fraction of executors to profile must be in [0,1]")
      .createWithDefault(0.1)

  private[profiler] val EXECUTOR_PROFILING_WRITE_INTERVAL =
    ConfigBuilder("spark.executor.profiling.writeInterval")
      .doc("Time interval in seconds after which the profiler output will be synced to dfs")
      .version("4.0.0")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ >= 0, "Write interval should be non-negative")
      .createWithDefault(30)

}
