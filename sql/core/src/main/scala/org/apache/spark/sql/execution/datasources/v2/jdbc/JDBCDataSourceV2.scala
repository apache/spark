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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{Table, TableProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class JDBCDataSourceV2 extends TableProvider with DataSourceRegister with Logging{

  override def shortName(): String = {
    logInfo("***dsv2-flows*** shortName - return connector name")
    "jdbcv2"
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    logInfo("***dsv2-flows*** getTable called")
    DBTable(SparkSession.active, options, None)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    logInfo("***dsv2-flows*** getTable called with schema")
    DBTable(SparkSession.active, options, Some(schema))
  }
}
