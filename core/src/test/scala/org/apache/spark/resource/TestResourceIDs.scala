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

package org.apache.spark.resource

import org.apache.spark.internal.config.{SPARK_DRIVER_PREFIX, SPARK_EXECUTOR_PREFIX, SPARK_TASK_PREFIX}
import org.apache.spark.resource.ResourceUtils.{FPGA, GPU}

object TestResourceIDs {
  val DRIVER_GPU_ID = ResourceID(SPARK_DRIVER_PREFIX, GPU)
  val EXECUTOR_GPU_ID = ResourceID(SPARK_EXECUTOR_PREFIX, GPU)
  val TASK_GPU_ID = ResourceID(SPARK_TASK_PREFIX, GPU)

  val DRIVER_FPGA_ID = ResourceID(SPARK_DRIVER_PREFIX, FPGA)
  val EXECUTOR_FPGA_ID = ResourceID(SPARK_EXECUTOR_PREFIX, FPGA)
  val TASK_FPGA_ID = ResourceID(SPARK_TASK_PREFIX, FPGA)
}
