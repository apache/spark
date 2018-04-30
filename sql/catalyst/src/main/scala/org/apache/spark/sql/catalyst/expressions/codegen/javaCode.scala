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

import java.lang.{Boolean => JBool}

import scala.collection.mutable
import scala.language.{existentials, implicitConversions}

import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * Trait representing an opaque fragments of java code.
 */
trait JavaCode {
  def code: String
  override def toString: String = code
}

/**
 * Utility functions for creating [[JavaCode]] fragments.
 */
object JavaCode {
  /**
   * Create a java literal.
   */
  def literal(v: String, dataType: DataType): LiteralValue = dataType match {
    case BooleanType if v == "true" => TrueLiteral
    case BooleanType if v == "false" => FalseLiteral
    case _ => new LiteralValue(v, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a default literal. This is null for reference types, false for boolean types and
   * -1 for other primitive types.
   */
  def defaultLiteral(dataType: DataType): LiteralValue = {
    new LiteralValue(
      CodeGenerator.defaultValue(dataType, typedNull = true),
      CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a local java variable.
   */
  def variable(name: String, dataType: DataType): VariableValue = {
    variable(name, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a local java variable.
   */
  def variable(name: String, javaClass: Class[_]): VariableValue = {
    VariableValue(name, javaClass)
  }

  /**
   * Create a local isNull variable.
   */
  def isNullVariable(name: String): VariableValue = variable(name, BooleanType)

  /**
   * Create a global java variable.
   */
  def global(name: String, dataType: DataType): GlobalValue = {
    global(name, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a global java variable.
   */
  def global(name: String, javaClass: Class[_]): GlobalValue = {
    GlobalValue(name, javaClass)
  }

  /**
   * Create a global isNull variable.
   */
  def isNullGlobal(name: String): GlobalValue = global(name, BooleanType)

  /**
   * Create an expression fragment.
   */
  def expression(code: String, dataType: DataType): SimpleExprValue = {
    expression(code, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create an expression fragment.
   */
  def expression(code: String, javaClass: Class[_]): SimpleExprValue = {
    SimpleExprValue(code, javaClass)
  }

  /**
   * Create a isNull expression fragment.
   */
  def isNullExpression(code: String): SimpleExprValue = {
    expression(code, BooleanType)
  }
}

/**
 * A typed java fragment that must be a valid java expression.
 */
trait ExprValue extends JavaCode {
  def javaType: Class[_]
  def isPrimitive: Boolean = javaType.isPrimitive

  // This will be called during string interpolation.
  override def toString: String = ExprValue.exprValueToString(this)
}

object ExprValue {

  private var currentHandlerForExprValue =
    new ThreadLocal[(ExprValue) => String]() {
      override def initialValue(): (ExprValue) => String =
        defaultExprValueToString
    }

  def defaultExprValueToString(exprValue: ExprValue): String = exprValue.code

  implicit def exprValueToString(exprValue: ExprValue): String = {
    currentListeners.get.foreach(_.genExprValue(exprValue))
    currentHandlerForExprValue.get()(exprValue)
  }

  protected[sql] val currentListeners =
    new ThreadLocal[mutable.ArrayBuffer[ExprValueCodegenListener]]() {
      override def initialValue(): mutable.ArrayBuffer[ExprValueCodegenListener] =
        mutable.ArrayBuffer.empty[ExprValueCodegenListener]
    }

  /**
   * By adding a listener, we can catch all expr values which are generated during codegen.
   */
  def withExprValueCodegenListener[T](listener: ExprValueCodegenListener)
      (f: () => T): T = {
    val index = currentListeners.get.length
    currentListeners.get += listener
    val result = f()
    assert(currentListeners.get.length == index + 1,
      "There are codegen listeners which are not removed after used!")
    currentListeners.get.remove(index)
    result
  }

  /**
   * Sets up a handler function used to convert `ExprValue` to string during codegen. The given
   * handler can overwrite the default behavior which outputs `ExprValue` to its actual code.
   */
  def withExprValueHandler[T](handlerFunc: (ExprValue) => String)(f: () => T): T = {
    val previousHandler = currentHandlerForExprValue.get
    currentHandlerForExprValue.set(handlerFunc)
    val result = f()
    currentHandlerForExprValue.set(previousHandler)
    result
  }
}

/**
 * A listener which gets notified when `ExprValue`s are generated to Java code.
 */
trait ExprValueCodegenListener {
  def genExprValue(exprValue: ExprValue): Unit
}

/**
 * A codegen listener specified for `SimpleExprValue` expression values.
 */
case class SimpleExprValueCodegenListener() extends ExprValueCodegenListener {
  protected[sql] val exprValues: mutable.HashSet[SimpleExprValue] =
    mutable.HashSet.empty[SimpleExprValue]

  override def genExprValue(exprValue: ExprValue): Unit = exprValue match {
    case s: SimpleExprValue => exprValues += s
    case _ =>
  }
}

/**
 * A java expression fragment.
 */
case class SimpleExprValue(expr: String, javaType: Class[_]) extends ExprValue {
  override def code: String = s"($expr)"
}

/**
 * A local variable java expression.
 */
case class VariableValue(variableName: String, javaType: Class[_]) extends ExprValue {
  override def code: String = variableName
}

/**
 * A global variable java expression.
 */
case class GlobalValue(value: String, javaType: Class[_]) extends ExprValue {
  override def code: String = value
}

/**
 * A literal java expression.
 */
class LiteralValue(val value: String, val javaType: Class[_]) extends ExprValue with Serializable {
  override def code: String = value

  override def equals(arg: Any): Boolean = arg match {
    case l: LiteralValue => l.javaType == javaType && l.value == value
    case _ => false
  }

  override def hashCode(): Int = value.hashCode() * 31 + javaType.hashCode()
}

case object TrueLiteral extends LiteralValue("true", JBool.TYPE)
case object FalseLiteral extends LiteralValue("false", JBool.TYPE)
