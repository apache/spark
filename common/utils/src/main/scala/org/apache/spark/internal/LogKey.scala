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
  val ACCUMULATOR_ID = Value
  val ANALYSIS_ERROR = Value
  val APP_DESC = Value
  val APP_ID = Value
  val APP_STATE = Value
  val BATCH_ID = Value
  val BLOCK_ID = Value
  val BLOCK_MANAGER_ID = Value
  val BROADCAST_ID = Value
  val BUCKET = Value
  val BYTECODE_SIZE = Value
  val CACHE_AUTO_REMOVED_SIZE = Value
  val CACHE_UNTIL_HIGHEST_CONSUMED_SIZE = Value
  val CACHE_UNTIL_LAST_PRODUCED_SIZE = Value
  val CATEGORICAL_FEATURES = Value
  val CLASS_LOADER = Value
  val CLASS_NAME = Value
  val CLUSTER_ID = Value
  val CODEC_LEVEL = Value
  val CODEC_NAME = Value
  val COLUMN_DATA_TYPE_SOURCE = Value
  val COLUMN_DATA_TYPE_TARGET = Value
  val COLUMN_DEFAULT_VALUE = Value
  val COLUMN_NAME = Value
  val COMMAND = Value
  val COMMAND_OUTPUT = Value
  val COMPONENT = Value
  val CONFIG = Value
  val CONFIG2 = Value
  val CONFIG3 = Value
  val CONFIG4 = Value
  val CONFIG5 = Value
  val CONSUMER = Value
  val CONTAINER = Value
  val CONTAINER_ID = Value
  val COUNT = Value
  val CSV_HEADER_COLUMN_NAME = Value
  val CSV_HEADER_COLUMN_NAMES = Value
  val CSV_HEADER_LENGTH = Value
  val CSV_SCHEMA_FIELD_NAME = Value
  val CSV_SCHEMA_FIELD_NAMES = Value
  val CSV_SOURCE = Value
  val DATA = Value
  val DATABASE_NAME = Value
  val DATAFRAME_CACHE_ENTRY = Value
  val DATAFRAME_ID = Value
  val DESCRIPTION = Value
  val DRIVER_ID = Value
  val DROPPED_PARTITIONS = Value
  val DURATION = Value
  val END_POINT = Value
  val ENGINE = Value
  val ERROR = Value
  val EVENT_LOOP = Value
  val EVENT_QUEUE = Value
  val EXECUTE_INFO = Value
  val EXECUTE_KEY = Value
  val EXECUTOR_ENV_REGEX = Value
  val EXECUTOR_ID = Value
  val EXECUTOR_IDS = Value
  val EXECUTOR_STATE = Value
  val EXIT_CODE = Value
  val EXPRESSION_TERMS = Value
  val FAILURES = Value
  val FALLBACK_VERSION = Value
  val FIELD_NAME = Value
  val FILE_FORMAT = Value
  val FILE_FORMAT2 = Value
  val FROM_OFFSET = Value
  val FUNCTION_NAME = Value
  val FUNCTION_PARAMETER = Value
  val GROUP_ID = Value
  val HADOOP_VERSION = Value
  val HISTORY_DIR = Value
  val HIVE_CLIENT_VERSION = Value
  val HIVE_METASTORE_VERSION = Value
  val HIVE_OPERATION_STATE = Value
  val HIVE_OPERATION_TYPE = Value
  val HOST = Value
  val HOST_PORT = Value
  val INCOMPATIBLE_TYPES = Value
  val INDEX = Value
  val INFERENCE_MODE = Value
  val INITIAL_CAPACITY = Value
  val INTERVAL = Value
  val JOB_ID = Value
  val JOIN_CONDITION = Value
  val JOIN_CONDITION_SUB_EXPRESSION = Value
  val KAFKA_PULLS_COUNT = Value
  val KAFKA_RECORDS_PULLED_COUNT = Value
  val KEY = Value
  val LAST_ACCESS_TIME = Value
  val LEARNING_RATE = Value
  val LINE = Value
  val LINE_NUM = Value
  val LISTENER = Value
  val LOAD_FACTOR = Value
  val LOG_TYPE = Value
  val MASTER_URL = Value
  val MAX_ATTEMPTS = Value
  val MAX_CACHE_UNTIL_HIGHEST_CONSUMED_SIZE = Value
  val MAX_CACHE_UNTIL_LAST_PRODUCED_SIZE = Value
  val MAX_CAPACITY = Value
  val MAX_CATEGORIES = Value
  val MAX_EXECUTOR_FAILURES = Value
  val MAX_SIZE = Value
  val MERGE_DIR_NAME = Value
  val MESSAGE = Value
  val METHOD_NAME = Value
  val MIN_SIZE = Value
  val NEW_VALUE = Value
  val NUM_COLUMNS = Value
  val NUM_ITERATIONS = Value
  val OBJECT_ID = Value
  val OFFSET = Value
  val OFFSETS = Value
  val OLD_BLOCK_MANAGER_ID = Value
  val OLD_VALUE = Value
  val OPTIMIZER_CLASS_NAME = Value
  val OP_ID = Value
  val OP_TYPE = Value
  val PARSE_MODE = Value
  val PARTITION_ID = Value
  val PARTITION_SPECIFICATION = Value
  val PARTITION_SPECS = Value
  val PATH = Value
  val PATHS = Value
  val POD_ID = Value
  val POD_NAME = Value
  val POD_NAMESPACE = Value
  val POD_PHASE = Value
  val POLICY = Value
  val PORT = Value
  val PRODUCER_ID = Value
  val PROVIDER = Value
  val QUERY_CACHE_VALUE = Value
  val QUERY_HINT = Value
  val QUERY_ID = Value
  val QUERY_PLAN = Value
  val QUERY_PLAN_COMPARISON = Value
  val QUERY_PLAN_LENGTH_ACTUAL = Value
  val QUERY_PLAN_LENGTH_MAX = Value
  val RANGE = Value
  val RDD_ID = Value
  val REASON = Value
  val REATTACHABLE = Value
  val RECEIVED_BLOCK_INFO = Value
  val REDUCE_ID = Value
  val RELATION_NAME = Value
  val REMAINING_PARTITIONS = Value
  val RESOURCE_NAME = Value
  val RETRY_COUNT = Value
  val RETRY_INTERVAL = Value
  val RULE_BATCH_NAME = Value
  val RULE_NAME = Value
  val RULE_NUMBER_OF_RUNS = Value
  val RUN_ID = Value
  val SCHEMA = Value
  val SCHEMA2 = Value
  val SERVICE_NAME = Value
  val SESSION_HOLD_INFO = Value
  val SESSION_ID = Value
  val SESSION_KEY = Value
  val SHARD_ID = Value
  val SHUFFLE_BLOCK_INFO = Value
  val SHUFFLE_ID = Value
  val SHUFFLE_MERGE_ID = Value
  val SIZE = Value
  val SLEEP_TIME = Value
  val SQL_TEXT = Value
  val STAGE_ID = Value
  val STATEMENT_ID = Value
  val STATUS = Value
  val STREAM_ID = Value
  val STREAM_NAME = Value
  val SUBMISSION_ID = Value
  val SUBSAMPLING_RATE = Value
  val TABLE_NAME = Value
  val TASK_ATTEMPT_ID = Value
  val TASK_ID = Value
  val TASK_NAME = Value
  val TASK_SET_NAME = Value
  val TASK_STATE = Value
  val THREAD = Value
  val THREAD_NAME = Value
  val TID = Value
  val TIME = Value
  val TIMEOUT = Value
  val TIME_UNITS = Value
  val TIP = Value
  val TOPIC = Value
  val TOPIC_PARTITION = Value
  val TOPIC_PARTITIONS = Value
  val TOPIC_PARTITION_OFFSET = Value
  val TOPIC_PARTITION_OFFSET_RANGE = Value
  val TOTAL_EFFECTIVE_TIME = Value
  val TOTAL_RECORDS_READ = Value
  val TOTAL_SIZE = Value
  val TOTAL_TIME = Value
  val TOTAL_TIME_READ = Value
  val UNSUPPORTED_EXPRESSION = Value
  val UNSUPPORTED_HINT_REASON = Value
  val UNTIL_OFFSET = Value
  val URI = Value
  val USER_ID = Value
  val USER_NAME = Value
  val WAIT_RESULT_TIME = Value
  val WAIT_SEND_TIME = Value
  val WAIT_TIME = Value
  val WATERMARK_CONSTRAINT = Value
  val WORKER_URL = Value
  val XSD_PATH = Value

  type LogKey = Value
}
