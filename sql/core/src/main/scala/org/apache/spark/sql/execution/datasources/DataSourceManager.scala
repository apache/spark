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

import org.apache.spark.api.python.PythonUtils
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.DATA_SOURCE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.python.UserDefinedPythonDataSource
import org.apache.spark.util.Utils


/**
 * A manager for user-defined data sources. It is used to register and lookup data sources by
 * their short names or fully qualified names.
 */
class DataSourceManager extends Logging {
  import DataSourceManager._

  // Lazy to avoid being invoked during Session initialization.
  // Otherwise, it goes infinite loop, session -> Python runner -> SQLConf -> session.
  private lazy val staticDataSourceBuilders = initialStaticDataSourceBuilders

  private val runtimeDataSourceBuilders =
    new ConcurrentHashMap[String, UserDefinedPythonDataSource]()

  /**
   * Register a data source builder for the given provider.
   * Note that the provider name is case-insensitive.
   */
  def registerDataSource(name: String, source: UserDefinedPythonDataSource): Unit = {
    val normalizedName = normalize(name)
    if (staticDataSourceBuilders.contains(normalizedName)) {
      // Cannot overwrite static Python Data Sources.
      throw QueryCompilationErrors.dataSourceAlreadyExists(name)
    }
    val previousValue = runtimeDataSourceBuilders.put(normalizedName, source)
    if (previousValue != null) {
      logWarning(log"The data source ${MDC(DATA_SOURCE, name)} replaced a previously " +
        log"registered data source.")
    }
  }

  /**
   * Returns a data source builder for the given provider and throw an exception if
   * it does not exist.
   */
  def lookupDataSource(name: String): UserDefinedPythonDataSource = {
    if (dataSourceExists(name)) {
      val normalizedName = normalize(name)
      staticDataSourceBuilders.getOrElse(
        normalizedName, runtimeDataSourceBuilders.get(normalizedName))
    } else {
      throw QueryCompilationErrors.dataSourceDoesNotExist(name)
    }
  }

  /**
   * Checks if a data source with the specified name exists (case-insensitive).
   */
  def dataSourceExists(name: String): Boolean = {
    val normalizedName = normalize(name)
    staticDataSourceBuilders.contains(normalizedName) ||
      runtimeDataSourceBuilders.containsKey(normalizedName)
  }

  override def clone(): DataSourceManager = {
    val manager = new DataSourceManager
    runtimeDataSourceBuilders.forEach((k, v) => manager.registerDataSource(k, v))
    manager
  }
}


object DataSourceManager extends Logging {
  // Visible for testing
  private[spark] var dataSourceBuilders: Option[Map[String, UserDefinedPythonDataSource]] = None
  private lazy val shouldLoadPythonDataSources: Boolean = {
    Utils.checkCommandAvailable(PythonUtils.defaultPythonExec)
  }

  private def normalize(name: String): String = name.toLowerCase(Locale.ROOT)

  private def initialStaticDataSourceBuilders: Map[String, UserDefinedPythonDataSource] = {
    if (shouldLoadPythonDataSources) this.synchronized {
      logInfo("Loading static Python Data Sources.")
      if (dataSourceBuilders.isEmpty) {
        val maybeResult = try {
          Some(UserDefinedPythonDataSource.lookupAllDataSourcesInPython())
        } catch {
          case e: Throwable if e.toString.contains(
              "ModuleNotFoundError: No module named 'pyspark'") =>
            // If PySpark is not in the Python path at all, suppress the warning
            // To make it less noisy, see also SPARK-47311.
            None
          case e: Throwable =>
            // Even if it fails for whatever reason, we shouldn't make the whole
            // application fail.
            logWarning(
              "Skipping the lookup of Python Data Sources due to the failure.", e)
            None
        }

        dataSourceBuilders = maybeResult.map { result =>
          result.names.zip(result.dataSources).map { case (name, dataSource) =>
            normalize(name) ->
              UserDefinedPythonDataSource(PythonUtils.createPythonFunction(dataSource))
          }.toMap
        }
      }
      dataSourceBuilders.getOrElse(Map.empty)
    } else {
      Map.empty
    }
  }
}
