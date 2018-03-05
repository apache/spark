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

package org.apache.spark.sql.catalyst.expressions.codegen

import scala.language.implicitConversions

import org.apache.spark.sql.types.DataType

// An abstraction that represents the evaluation result of [[ExprCode]].
abstract class ExprValue {

  val javaType: String

  // Whether we can directly access the evaluation value anywhere.
  // For example, a variable created outside a method can not be accessed inside the method.
  // For such cases, we may need to pass the evaluation as parameter.
  val canDirectAccess: Boolean

  def isPrimitive: Boolean = CodeGenerator.isPrimitiveType(javaType)
}

object ExprValue {
  implicit def exprValueToString(exprValue: ExprValue): String = exprValue.toString
}

// A literal evaluation of [[ExprCode]].
class LiteralValue(val value: String, val javaType: String) extends ExprValue {
  override def toString: String = value
  override val canDirectAccess: Boolean = true
}

object LiteralValue {
  def apply(value: String, javaType: String): LiteralValue = new LiteralValue(value, javaType)
  def unapply(literal: LiteralValue): Option[(String, String)] =
    Some((literal.value, literal.javaType))
}

// A variable evaluation of [[ExprCode]].
case class VariableValue(
    val variableName: String,
    val javaType: String) extends ExprValue {
  override def toString: String = variableName
  override val canDirectAccess: Boolean = false
}

// A statement evaluation of [[ExprCode]].
case class StatementValue(
    val statement: String,
    val javaType: String,
    val canDirectAccess: Boolean = false) extends ExprValue {
  override def toString: String = statement
}

// A global variable evaluation of [[ExprCode]].
case class GlobalValue(val value: String, val javaType: String) extends ExprValue {
  override def toString: String = value
  override val canDirectAccess: Boolean = true
}

case object TrueLiteral extends LiteralValue("true", "boolean")
case object FalseLiteral extends LiteralValue("false", "boolean")
