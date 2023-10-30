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

package org.apache.spark.sql.execution.datasources

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceManager {

  private type DataSourceBuilder = (
    SparkSession,  // Spark session
    String,  // provider name
    Seq[String],  // paths
    Option[StructType],  // user specified schema
    CaseInsensitiveStringMap  // options
  ) => LogicalPlan

  private val dataSourceBuilders = new ConcurrentHashMap[String, DataSourceBuilder]()

  private def normalize(name: String): String = name.toLowerCase(Locale.ROOT)

  def registerDataSource(name: String, builder: DataSourceBuilder): Unit = {
    val normalizedName = normalize(name)
    if (dataSourceBuilders.containsKey(normalizedName)) {
      throw QueryCompilationErrors.dataSourceAlreadyExists(name)
    }
    // TODO(SPARK-45639): check if the data source is a DSv1 or DSv2 using loadDataSource.
    dataSourceBuilders.put(normalizedName, builder)
  }

  def dataSourceExists(name: String): Boolean =
    dataSourceBuilders.containsKey(normalize(name))
}
