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

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._


/**
 * A function that calculates bitwise and(&) of two numbers.
 *
 * Code generation inherited from BinaryArithmetic.
 */
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns the result of bitwise AND of `expr1` and `expr2`.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 5;
       1
  """)
case class BitwiseAnd(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "&"

  private lazy val and: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 & evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 & evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
  }

  protected override def nullSafeEval(input1: Any, input2: Any): Any = and(input1, input2)
}

/**
 * A function that calculates bitwise or(|) of two numbers.
 *
 * Code generation inherited from BinaryArithmetic.
 */
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns the result of bitwise OR of `expr1` and `expr2`.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 5;
       7
  """)
case class BitwiseOr(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "|"

  private lazy val or: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 | evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 | evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
  }

  protected override def nullSafeEval(input1: Any, input2: Any): Any = or(input1, input2)
}

/**
 * A function that calculates bitwise xor({@literal ^}) of two numbers.
 *
 * Code generation inherited from BinaryArithmetic.
 */
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns the result of bitwise exclusive OR of `expr1` and `expr2`.",
  examples = """
    Examples:
      > SELECT 3 _FUNC_ 5;
       2
  """)
case class BitwiseXor(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "^"

  private lazy val xor: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 ^ evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 ^ evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
  }

  protected override def nullSafeEval(input1: Any, input2: Any): Any = xor(input1, input2)
}

/**
 * A function that calculates bitwise not(~) of a number.
 */
@ExpressionDescription(
  usage = "_FUNC_ expr - Returns the result of bitwise NOT of `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_ 0;
       -1
  """)
case class BitwiseNot(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  override def dataType: DataType = child.dataType

  override def toString: String = s"~$child"

  private lazy val not: (Any) => Any = dataType match {
    case ByteType =>
      ((evalE: Byte) => (~evalE).toByte).asInstanceOf[(Any) => Any]
    case ShortType =>
      ((evalE: Short) => (~evalE).toShort).asInstanceOf[(Any) => Any]
    case IntegerType =>
      ((evalE: Int) => ~evalE).asInstanceOf[(Any) => Any]
    case LongType =>
      ((evalE: Long) => ~evalE).asInstanceOf[(Any) => Any]
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"(${CodeGenerator.javaType(dataType)}) ~($c)")
  }

  protected override def nullSafeEval(input: Any): Any = not(input)

  override def sql: String = s"~${child.sql}"
}
