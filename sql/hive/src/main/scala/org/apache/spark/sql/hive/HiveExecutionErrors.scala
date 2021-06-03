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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.metadata.HiveException

/**
 * Object for grouping error messages from (most) exceptions thrown during
 * hive execution.
 */
object HiveExecutionErrors {
  def cannotFetchTablesOfDatabaseError(dbName: String, e: Exception): Throwable = {
    new HiveException(s"Unable to fetch tables of db $dbName", e)
  }

  def illegalLocationClauseForViewPartitionError(): Throwable = {
    new HiveException("LOCATION clause illegal for view partition")
  }
}
