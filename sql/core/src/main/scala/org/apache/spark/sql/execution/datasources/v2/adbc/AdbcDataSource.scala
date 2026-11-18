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

package org.apache.spark.sql.execution.datasources.v2.adbc

import java.util

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A data source that reads from databases through ADBC (Arrow Database Connectivity) drivers,
 * returning the drivers' Arrow batches to Spark as columnar batches.
 */
class AdbcDataSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "adbc"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    AdbcUtils.getSchema(new AdbcOptions(options.asCaseSensitiveMap().asScala.toMap))
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new AdbcTable(schema, new AdbcOptions(properties.asScala.toMap))
  }
}
