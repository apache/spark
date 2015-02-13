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

import java.util.{List => JList, Map => JMap}

import org.apache.spark.Accumulator
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.ScalaUdf
import org.apache.spark.sql.execution.PythonUDF
import org.apache.spark.sql.types.DataType

/**
 * A user-defined function. To create one, use the `udf` functions in [[Dsl]].
 * As an example:
 * {{{
 *   // Defined a UDF that returns true or false based on some numeric score.
 *   val predict = udf((score: Double) => if (score > 0.5) true else false)
 *
 *   // Projects a column that adds a prediction column based on the score column.
 *   df.select( predict(df("score")) )
 * }}}
 */
case class UserDefinedFunction(f: AnyRef, dataType: DataType) {

  def apply(exprs: Column*): Column = {
    Column(ScalaUdf(f, dataType, exprs.map(_.expr)))
  }
}

/**
 * A user-defined Python function. To create one, use the `pythonUDF` functions in [[Dsl]].
 * This is used by Python API.
 */
private[sql] case class UserDefinedPythonFunction(
    name: String,
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: Accumulator[JList[Array[Byte]]],
    dataType: DataType) {

  def apply(exprs: Column*): Column = {
    val udf = PythonUDF(name, command, envVars, pythonIncludes, pythonExec, broadcastVars,
      accumulator, dataType, exprs.map(_.expr))
    Column(udf)
  }
}
