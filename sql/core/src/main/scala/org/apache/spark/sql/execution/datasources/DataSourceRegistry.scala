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
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


trait DataSourceRegistry {

  type DataSourceBuilder = (
    SparkSession,  // Spark session
    String,  // provider name
    Seq[String],  // paths
    Option[StructType],  // user specified schema
    CaseInsensitiveStringMap  // options
  ) => LogicalPlan

  def registerDataSource(name: String, builder: DataSourceBuilder): Unit

  def dataSourceExists(name: String): Boolean

  /** Create a copy of this registry with identical data sources as this registry. */
  override def clone(): DataSourceRegistry = throw new CloneNotSupportedException()
}

class SimpleDataSourceRegistry extends DataSourceRegistry with Logging {
  @GuardedBy("this")
  protected val dataSourceBuilders = new mutable.HashMap[String, DataSourceBuilder]

  private def normalize(name: String): String = name.toLowerCase(Locale.ROOT)

  override def registerDataSource(name: String, builder: DataSourceBuilder): Unit = synchronized {
    val normalizedName = normalize(name)
    if (dataSourceBuilders.contains(normalizedName)) {
      throw QueryCompilationErrors.dataSourceAlreadyExists(name)
    }
    // TODO(SPARK-45639): check if the data source is a DSv1 or DSv2 using loadDataSource.
    dataSourceBuilders.put(normalizedName, builder)
  }

  override def dataSourceExists(name: String): Boolean = synchronized {
    dataSourceBuilders.contains(normalize(name))
  }

  override def clone(): SimpleDataSourceRegistry = {
    val registry = new SimpleDataSourceRegistry
    dataSourceBuilders.iterator.foreach { case (name, builder) =>
      registry.registerDataSource(name, builder)
    }
    registry
  }
}
