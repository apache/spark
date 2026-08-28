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
 * Builds a [[TranspiledJavaUDF]] column for the `java` transpiler in `pyspark.sql.transpile_java`.
 *
 * The Python side assembles Java source but has no way to name a Catalyst expression: it can only
 * build columns out of `pyspark.sql.functions`, and there is no function for this node. So it calls
 * here over Py4J, the way it would call any other JVM entry point. Data types cross as JSON, which
 * is what every other Python-to-JVM type hand-off in this package uses.
 *
 * This lives in `sql/core` rather than beside the expression because building a [[Column]] needs
 * the classic conversions, which catalyst cannot see.
 */
object TranspiledJavaUDFBuilder {

  /**
   * @param name the UDF's name, used for display and to seed the generated method's name
   * @param body Java statements ending in a `return`, reading the parameters named by `argNames`
   * @param argNames the generated method's parameter names, parallel to `children`
   * @param children the argument columns, which the caller has already cast to `inputTypesJson`
   * @param inputTypesJson one JSON data type per child, each within the expression's boxed ABI
   * @param returnTypeJson the JSON data type the body returns
   */
  def create(
      name: String,
      body: String,
      argNames: JList[String],
      children: JList[Column],
      inputTypesJson: JList[String],
      returnTypeJson: String): Column = {
    val expr = TranspiledJavaUDF(
      name = name,
      body = body,
      argNames = argNames.asScala.toSeq,
      children = children.asScala.map(ColumnConversions.expression).toSeq,
      inputTypes = inputTypesJson.asScala.map(DataType.fromJson).toSeq,
      dataType = DataType.fromJson(returnTypeJson))
    // Compile before handing the option back. A body that does not compile has to be refused here,
    // where the caller still has the interpreted UDF to fall back to -- `_transpile_func` turns the
    // exception into a skipped option. Later there is no fallback left, because the option has
    // replaced the Python UDF in the plan and both eval paths compile this same source.
    expr.validate()
    ClassicConversions.column(expr)
  }
}
