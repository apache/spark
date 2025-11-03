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

package org.apache.spark.sql.connect.ui

private[ui] object ToolTips {
  val SPARK_CONNECT_SERVER_FINISH_TIME =
    "Execution finish time, before fetching the results"

  val SPARK_CONNECT_SERVER_CLOSE_TIME =
    "Operation close time after fetching the results"

  val SPARK_CONNECT_SERVER_EXECUTION =
    "Difference between start time and finish time"

  val SPARK_CONNECT_SERVER_DURATION =
    "Difference between start time and close time"

  val SPARK_CONNECT_SESSION_TOTAL_EXECUTE =
    "Number of operations submitted in this session"

  val SPARK_CONNECT_SESSION_DURATION =
    "Elapsed time since session start, or until closed if the session was closed"

}
