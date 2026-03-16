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

package org.apache.spark.sql.catalyst.util

/**
 * A utility that holds constants for handling deltas of rows.
 */
object RowDeltaUtils {
  final val OPERATION_COLUMN: String = "__row_operation"
  final val DELETE_OPERATION: Int = 1
  final val UPDATE_OPERATION: Int = 2
  final val INSERT_OPERATION: Int = 3
  final val REINSERT_OPERATION: Int = 4
  final val WRITE_OPERATION: Int = 5
  final val WRITE_WITH_METADATA_OPERATION: Int = 6
  final val ORIGINAL_ROW_ID_VALUE_PREFIX: String = "__original_row_id_"
}
