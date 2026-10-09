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
package org.apache.spark.sql.execution.datasources.v2.ffi

import java.util

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.NamedTableProvider
import org.apache.spark.sql.execution.datasources.v2.columnar.ColumnarTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The Data Source V2 provider of the native data sources: data sources implemented by a native
 * library through `org.apache.spark.sql.datasource.NativeBridge`.
 */
class NativeDataSourceV2 extends NamedTableProvider {
  private var shortName: String = _

  override def setShortName(name: String): Unit = {
    assert(shortName == null)
    shortName = name
  }

  // The name of the data source, and where its library is. Resolved on first use, which is while
  // the query is analyzed, with the configuration of its session.
  private lazy val resolved: (String, NativeLibraryLocation) = {
    assert(shortName != null)
    val (name, location) = NativeDataSourceRegistry.lookup(shortName, SQLConf.get).getOrElse {
      throw QueryCompilationErrors.dataSourceDoesNotExist(shortName)
    }
    (name, NativeDataSourceRegistry.distribute(location))
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val (name, location) = resolved
    new NativeDataSource(location, name, options).schema()
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val (name, location) = resolved
    new ColumnarTable(
      shortName, schema, properties, options => new NativeDataSource(location, name, options))
  }

  override def supportsExternalMetadata(): Boolean = true
}
