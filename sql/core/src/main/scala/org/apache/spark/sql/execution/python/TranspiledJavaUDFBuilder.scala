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

import java.util.{List => JList}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.TranspiledJavaUDF
import org.apache.spark.sql.classic.{ClassicConversions, ColumnConversions}
import org.apache.spark.sql.types.DataType

/**
 * Py4J entry point for the `java` UDF transpiler. In `sql/core` because building a [[Column]]
 * needs the classic conversions.
 */
object TranspiledJavaUDFBuilder {

  /**
   * @param name the UDF's name
   * @param body the method body, reading its arguments as `_udf_arg_0`, `_udf_arg_1`, ...
   * @param children the argument columns, already cast to `LongType` or `BooleanType`
   * @param returnTypeJson the body's result type as JSON
   */
  def create(
      name: String,
      body: String,
      children: JList[Column],
      returnTypeJson: String): Column = {
    val expr = TranspiledJavaUDF(
      name, body, children.asScala.map(ColumnConversions.expression).toSeq,
      DataType.fromJson(returnTypeJson))
    // Compile now, while a failure can still skip this option.
    expr.validate()
    ClassicConversions.column(expr)
  }
}
