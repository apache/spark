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
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.execution.PythonUDF
import org.apache.spark.sql.types.DataType

/**
 * A user-defined function. To create one, use the `udf` functions in [[functions]].
 * As an example:
 * {{{
 *   // Defined a UDF that returns true or false based on some numeric score.
 *   val predict = udf((score: Double) => if (score > 0.5) true else false)
 *
 *   // Projects a column that adds a prediction column based on the score column.
 *   df.select( predict(df("score")) )
 * }}}
 *
 * @since 1.3.0
 */
@Experimental
case class UserDefinedFunction protected[sql] (
    f: AnyRef,
    dataType: DataType,
    inputTypes: Seq[DataType] = Nil) {

  def apply(exprs: Column*): Column = {
    Column(ScalaUDF(f, dataType, exprs.map(_.expr), inputTypes))
  }
}

/**
 * A user-defined Python function. To create one, use the `pythonUDF` functions in [[functions]].
 * This is used by Python API.
 */
private[sql] case class UserDefinedPythonFunction(
    name: String,
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    pythonVer: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: Accumulator[JList[Array[Byte]]],
    dataType: DataType) {

  def builder(e: Seq[Expression]): PythonUDF = {
    PythonUDF(name, command, envVars, pythonIncludes, pythonExec, pythonVer, broadcastVars,
      accumulator, dataType, e)
  }

  /** Returns a [[Column]] that will evaluate to calling this UDF with the given input. */
  def apply(exprs: Column*): Column = {
    val udf = builder(exprs.map(_.expr))
    Column(udf)
  }
}
