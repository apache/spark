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

package org.apache.spark.sql.execution.python

import org.apache.spark.Accumulator
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

/**
 * A user-defined Python function. This is used by the Python API.
 */
case class UserDefinedPythonFunction(
    name: String,
    command: Array[Byte],
    envVars: java.util.Map[String, String],
    pythonIncludes: java.util.List[String],
    pythonExec: String,
    pythonVer: String,
    broadcastVars: java.util.List[Broadcast[PythonBroadcast]],
    accumulator: Accumulator[java.util.List[Array[Byte]]],
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
