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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A function that marks a given expression with specified collation.
 * This function is pass-through, it will not modify the input data.
 * Only type metadata will be updated.
 */
@ExpressionDescription(
  usage = "expr _FUNC_ collationName",
  examples = """
    Examples:
      > SELECT 'Spark SQL' COLLATE 'UCS_BASIC_LCASE';
       Spark SQL
  """,
  since = "4.0.0",
  group = "string_funcs")
case class Collate(child: Expression, collationName: String)
  extends UnaryExpression with ExpectsInputTypes {
  private val collationId = CollationFactory.collationNameToId(collationName)
  override def dataType: DataType = StringType(collationId)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override protected def withNewChildInternal(
    newChild: Expression): Expression = copy(newChild)

  override def eval(row: InternalRow): Any = child.eval(row)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (in) => in)
}

/**
 * A function that returns the collation name of a given expression.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr)",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL');
       UCS_BASIC
  """,
  since = "4.0.0",
  group = "string_funcs")
case class Collation(child: Expression) extends UnaryExpression {
  override def dataType: DataType = StringType
  override protected def withNewChildInternal(newChild: Expression): Collation = copy(newChild)

  override def eval(row: InternalRow): Any = {
    val collationId = child.dataType.asInstanceOf[StringType].collationId
    UTF8String.fromString(CollationFactory.fetchCollation(collationId).collationName)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val collationId = child.dataType.asInstanceOf[StringType].collationId
    val collationName = CollationFactory.fetchCollation(collationId).collationName
    defineCodeGen(ctx, ev, _ => s"""UTF8String.fromString("$collationName")""")
  }
}
