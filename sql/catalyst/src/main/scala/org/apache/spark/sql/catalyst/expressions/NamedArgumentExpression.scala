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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.DataType

/**
 * This represents an argument expression to a routine call accompanied with an
 * explicit reference to the corresponding argument name as a string. In this way,
 * the analyzer can make sure that the provided values match up to the arguments
 * as intended, and the arguments may appear in any order.
 * This unary expression is unevaluable because we intend to replace it with
 * the provided value itself during query analysis (after possibly rearranging
 * the parsed argument list to match up the names to the expected routine
 * signature).
 *
 * SQL Syntax: key => value
 * SQL grammar: key=identifier FAT_ARROW value=expression
 *
 * Example usage with the "encode" scalar function:
 * SELECT encode("abc", charset => "utf-8");
 *   The second argument generates NamedArgumentExpression("charset", Literal("utf-8"))
 * SELECT encode(charset => "utf-8", value => "abc");
 *
 * @param key The name of the routine argument
 * @param value The value of the routine argument
 */
case class NamedArgumentExpression(key: String, value: Expression)
  extends UnaryExpression with Unevaluable {

  override def dataType: DataType = value.dataType

  override def toString: String = s"$key => $value"

  // NamedArgumentExpression has a single child, which is its value expression,
  // so the value expression can be resolved by Analyzer rules recursively.
  // For example, when the value is a built-in function expression,
  // it must be resolved by [[ResolveFunctions]]
  override def child: Expression = value

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(value = newChild)
}
