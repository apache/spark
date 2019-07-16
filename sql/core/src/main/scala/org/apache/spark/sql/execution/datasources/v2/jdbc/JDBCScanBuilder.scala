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

package org.apache.spark.sql.execution.datasources.v2.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.SupportsRead
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType

class JDBCScanBuilder extends ScanBuilder with
  SupportsPushDownFilters with SupportsPushDownRequiredColumns
  with Logging {

  var specifiedFilters: Array[Filter] = Array.empty


  def build: Scan = {
    logInfo("***dsv2-flows*** Scan called")
  new DBTableScan()

  }

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logInfo("***dsv2-flows*** PushDown filters called")
    specifiedFilters = filters
    filters
  }

  def pruneColumns(requiredSchema: StructType): Unit = {
    logInfo("***dsv2-flows*** pruneColumns called")

  }

  def pushedFilters: Array[Filter] = {
    logInfo("***dsv2-flows*** pushedFilters called")
    specifiedFilters
  }

}
