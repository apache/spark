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
  val APPLICATION_ID = Value
  val APPLICATION_STATE = Value
  val BUCKET = Value
  val BYTECODE_SIZE = Value
  val CLASS_NAME = Value
  val CONTAINER_ID = Value
  val EXECUTOR_ID = Value
  val EXIT_CODE = Value
  val FORMATTED_CODE = Value
  val JOIN_CONDITION = Value
  val MAX_EXECUTOR_FAILURES = Value
  val MAX_SIZE = Value
  val METHOD_NAME = Value
  val MIN_SIZE = Value
  val REMOTE_ADDRESS = Value
  val POD_ID = Value
  val QUERY_PLAN = Value
  val RULE_BATCH_NAME = Value
  val RULE_NAME = Value
  val RULE_NUMBER_OF_RUNS = Value
  val TIME_UNITS = Value
  val WATERMARK_CONSTRAINT = Value
  val XSD_PATH = Value

  type LogKey = Value
}
