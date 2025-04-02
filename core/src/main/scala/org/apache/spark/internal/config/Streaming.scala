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

import java.util.concurrent.TimeUnit

private[spark] object Streaming {

  private[spark] val STREAMING_DYN_ALLOCATION_ENABLED =
    ConfigBuilder("spark.streaming.dynamicAllocation.enabled")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val STREAMING_DYN_ALLOCATION_TESTING =
    ConfigBuilder("spark.streaming.dynamicAllocation.testing")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val STREAMING_DYN_ALLOCATION_MIN_EXECUTORS =
    ConfigBuilder("spark.streaming.dynamicAllocation.minExecutors")
      .version("3.0.0")
      .intConf
      .checkValue(_ > 0, "The min executor number of streaming dynamic " +
        "allocation must be positive.")
      .createOptional

  private[spark] val STREAMING_DYN_ALLOCATION_MAX_EXECUTORS =
    ConfigBuilder("spark.streaming.dynamicAllocation.maxExecutors")
      .version("3.0.0")
      .intConf
      .checkValue(_ > 0, "The max executor number of streaming dynamic " +
        "allocation must be positive.")
      .createWithDefault(Int.MaxValue)

  private[spark] val STREAMING_DYN_ALLOCATION_SCALING_INTERVAL =
    ConfigBuilder("spark.streaming.dynamicAllocation.scalingInterval")
      .version("3.0.0")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ > 0, "The scaling interval of streaming dynamic " +
        "allocation must be positive.")
      .createWithDefault(60)

  private[spark] val STREAMING_DYN_ALLOCATION_SCALING_UP_RATIO =
    ConfigBuilder("spark.streaming.dynamicAllocation.scalingUpRatio")
      .version("3.0.0")
      .doubleConf
      .checkValue(_ > 0, "The scaling up ratio of streaming dynamic " +
        "allocation must be positive.")
      .createWithDefault(0.9)

  private[spark] val STREAMING_DYN_ALLOCATION_SCALING_DOWN_RATIO =
    ConfigBuilder("spark.streaming.dynamicAllocation.scalingDownRatio")
      .version("3.0.0")
      .doubleConf
      .checkValue(_ > 0, "The scaling down ratio of streaming dynamic " +
        "allocation must be positive.")
      .createWithDefault(0.3)
}
