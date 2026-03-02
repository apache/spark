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
import org.apache.spark.sql.classic.{ColumnNodeExpression, ExpressionUtils}
import org.apache.spark.sql.types.DataType

/**
 * JVM-side builder for [[InProcessPythonUDF]] expressions, called from the Python API
 * via py4j's JVM reflection bridge (``sc._jvm.org.apache.spark...InProcessPythonUDFBuilder``).
 *
 * Accepts Java-typed arguments as passed by PySpark's ``sc._jvm`` proxy and returns a
 * [[Column]] backed by an [[InProcessPythonUDF]] expression.
 */
object InProcessPythonUDFBuilder {

  /**
   * Build a [[Column]] backed by an [[InProcessPythonUDF]] expression.
   *
   * @param name            display name (Python function ``__name__``)
   * @param serializedFunc  cloudpickle bytes of the Python UDF
   * @param returnTypeJson  JSON string of the Spark SQL return type
   * @param jColumns        Java List of JVM [[Column]] objects (the UDF inputs)
   * @param deterministic   whether the UDF always returns the same output for the same input;
   *                        set to false for UDFs that use randomness or external state
   * @return                [[Column]] backed by an [[InProcessPythonUDF]] expression
   */
  def build(
      name: String,
      serializedFunc: Array[Byte],
      returnTypeJson: String,
      jColumns: JList[Column],
      deterministic: Boolean): Column = {
    val returnType = DataType.fromJson(returnTypeJson)
    val inputExprs = jColumns.asScala.map(col => ColumnNodeExpression(col.node)).toSeq
    ExpressionUtils.column(
      InProcessPythonUDF(name, serializedFunc, inputExprs, returnType, deterministic))
  }
}
