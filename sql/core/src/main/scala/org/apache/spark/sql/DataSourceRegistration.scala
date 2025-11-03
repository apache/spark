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

package org.apache.spark.sql

import org.apache.spark.SparkClassNotFoundException
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.{DataSource, DataSourceManager}
import org.apache.spark.sql.execution.datasources.v2.python.UserDefinedPythonDataSource
import org.apache.spark.sql.internal.SQLConf

/**
 * Functions for registering user-defined data sources.
 * Use `SparkSession.dataSource` to access this.
 */
@Evolving
private[sql] class DataSourceRegistration private[sql] (dataSourceManager: DataSourceManager)
  extends Logging {

  protected[sql] def registerPython(
      name: String,
      dataSource: UserDefinedPythonDataSource): Unit = {
    log.debug(
      s"""
         | Registering new Python data source:
         | name: $name
         | command: ${dataSource.dataSourceCls.command}
         | envVars: ${dataSource.dataSourceCls.envVars}
         | pythonIncludes: ${dataSource.dataSourceCls.pythonIncludes}
         | pythonExec: ${dataSource.dataSourceCls.pythonExec}
      """.stripMargin)

    checkDataSourceExists(name)

    dataSourceManager.registerDataSource(name, dataSource)
  }

  /**
   * Checks if the specified data source exists.
   *
   * This method allows for user-defined data sources to be registered even if they
   * have the same name as an existing data source in the registry. However, if the
   * data source can be successfully loaded and is not a user-defined one, an error
   * is thrown to prevent lookup errors with built-in or Scala/Java data sources.
   */
  private def checkDataSourceExists(name: String): Unit = {
    // Allow re-registration of user-defined data sources.
    // TODO(SPARK-46616): disallow re-registration of statically registered data sources.
    if (dataSourceManager.dataSourceExists(name)) return

    try {
      DataSource.lookupDataSource(name, SQLConf.get)
      throw QueryCompilationErrors.dataSourceAlreadyExists(name)
    } catch {
      case e: SparkClassNotFoundException if e.getCondition == "DATA_SOURCE_NOT_FOUND" => // OK
      case _: Throwable =>
        // If there are other errors when resolving the data source, it's unclear whether
        // it's safe to proceed. To prevent potential lookup errors, treat it as an existing
        // data source and prevent re-registration.
        throw QueryCompilationErrors.dataSourceAlreadyExists(name)
    }
  }
}
