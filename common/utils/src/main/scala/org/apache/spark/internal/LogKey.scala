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

/**
 * Various keys used for mapped diagnostic contexts(MDC) in logging.
 * All structured logging keys should be defined here for standardization.
 */
object LogKey extends Enumeration {
  val APP_DESC = Value
  val APP_ID = Value
  val APP_STATE = Value
  val BLOCK_ID = Value
  val BLOCK_MANAGER_ID = Value
  val BROADCAST_ID = Value
  val BUCKET = Value
  val CLASS_LOADER = Value
  val CLASS_NAME = Value
  val COMMAND = Value
  val COMMAND_OUTPUT = Value
  val CONFIG = Value
  val CONFIG2 = Value
  val CONTAINER_ID = Value
  val COUNT = Value
  val DRIVER_ID = Value
  val ERROR = Value
  val EVENT_QUEUE = Value
  val EXECUTOR_ID = Value
  val EXECUTOR_STATE_CHANGED = Value
  val EXIT_CODE = Value
  val HOST = Value
  val JOB_ID = Value
  val LINE = Value
  val LINE_NUM = Value
  val MASTER_URL = Value
  val MAX_ATTEMPTS = Value
  val MAX_EXECUTOR_FAILURES = Value
  val MAX_SIZE = Value
  val MIN_SIZE = Value
  val OLD_BLOCK_MANAGER_ID = Value
  val PARTITION_ID = Value
  val PATH = Value
  val POD_ID = Value
  val REASON = Value
  val REMOTE_ADDRESS = Value
  val RETRY_COUNT = Value
  val RPC_ADDRESS = Value
  val SHUFFLE_ID = Value
  val SIZE = Value
  val STAGE_ID = Value
  val SUBMISSION_ID = Value
  val TASK_ATTEMPT_ID = Value
  val TASK_ID = Value
  val TASK_NAME = Value
  val TASK_SET_NAME = Value
  val TASK_STATE = Value
  val TID = Value
  val TIMEOUT = Value
  val WORKER_URL = Value

  type LogKey = Value
}
