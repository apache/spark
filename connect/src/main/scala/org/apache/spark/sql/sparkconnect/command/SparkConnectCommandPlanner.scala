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

package org.apache.spark.sql.sparkconnect.command

import com.google.common.collect.{Lists, Maps}

import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.connect.{proto => proto}
import org.apache.spark.connect.proto.CreateScalarFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types.StringType

case class SparkConnectCommandPlanner(session: SparkSession, command: proto.Command) {

  def process(): Unit = {
    command.commandType match {
      case proto.Command.CommandType.CreateFunction(cf) => handleCreateScalarFunction(cf)
      case _ => throw new UnsupportedOperationException(s"${command.commandType} not supported.")
    }
  }

  // This is a helper function that registers a new Python function in the
  // [[SparkSession]].
  def handleCreateScalarFunction(cf: CreateScalarFunction): Unit = {
    val function = SimplePythonFunction(
      cf.functionDefinition.serializedFunction.get.toByteArray,
      Maps.newHashMap(),
      Lists.newArrayList(),
      "python3",
      "3.9",
      Lists.newArrayList(),
      null)

    val udf = UserDefinedPythonFunction(
      cf.parts.head,
      function,
      StringType,
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = false)

    session.udf.registerPython(cf.parts.head, udf)

  }

}
