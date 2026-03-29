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
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry
import org.apache.spark.sql.execution.python.UserDefinedPythonTableFunction

/**
 * Functions for registering user-defined table functions. Use `SparkSession.udtf` to access this.
 *
 * @since 3.5.0
 */
@Evolving
private[sql] class UDTFRegistration private[sql] (tableFunctionRegistry: TableFunctionRegistry)
  extends Logging {

  protected[sql] def registerPython(name: String, udtf: UserDefinedPythonTableFunction): Unit = {
    log.debug(
      s"""
         | Registering new PythonUDTF:
         | name: $name
         | command: ${udtf.func.command}
         | envVars: ${udtf.func.envVars}
         | pythonIncludes: ${udtf.func.pythonIncludes}
         | pythonExec: ${udtf.func.pythonExec}
         | returnType: ${udtf.returnType}
         | udfDeterministic: ${udtf.udfDeterministic}
      """.stripMargin)

    tableFunctionRegistry.createOrReplaceTempFunction(
      name, udtf.builder(_, SparkSession.getActiveSession.get.sessionState.sqlParser),
      source = "python_udtf")
  }
}
