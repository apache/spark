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

import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.DataSourceManager
import org.apache.spark.sql.execution.python.UserDefinedPythonDataSource

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

    dataSourceManager.registerDataSource(name, dataSource)
  }
}
