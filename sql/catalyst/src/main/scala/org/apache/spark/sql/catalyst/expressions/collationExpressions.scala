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
import org.apache.spark.sql.catalyst.analysis.ExpressionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

// scalastyle:off line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr, collationName) - Marks a given expression with the specified collation.",
  arguments = """
    Arguments:
      * expr - String expression to perform collation on.
      * collationName - Foldable string expression that specifies the collation name.
  """,
  examples = """
    Examples:
      > SET spark.sql.collation.enabled=true;
      spark.sql.collation.enabled	true
      > SELECT COLLATION('Spark SQL' _FUNC_ UTF8_BINARY_LCASE);
      UTF8_BINARY_LCASE
      > SET spark.sql.collation.enabled=false;
      spark.sql.collation.enabled	false
  """,
  since = "4.0.0",
  group = "string_funcs")
// scalastyle:on line.contains.tab
object CollateExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    // We need to throw collationNotEnabledError before unexpectedNullError
    // and nonFoldableArgumentError, as we do not want user to see misleading
    // messages that collation is enabled
    if (!SQLConf.get.collationEnabled) {
      throw QueryCompilationErrors.collationNotEnabledError()
    }
    expressions match {
      case Seq(e: Expression, collationExpr: Expression) =>
        (collationExpr.dataType, collationExpr.foldable) match {
          case (StringType, true) =>
            val evalCollation = collationExpr.eval()
            if (evalCollation == null) {
              throw QueryCompilationErrors.unexpectedNullError("collation", collationExpr)
            } else {
              Collate(e, evalCollation.toString)
            }
          case (StringType, false) => throw QueryCompilationErrors.nonFoldableArgumentError(
            funcName, "collationName", StringType)
          case (_, _) => throw QueryCompilationErrors.unexpectedInputDataTypeError(
            funcName, 1, StringType, collationExpr)
        }
      case s => throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(2), s.length)
    }
  }
}

/**
 * An expression that marks a given expression with specified collation.
 * This function is pass-through, it will not modify the input data.
 * Only type metadata will be updated.
 */
case class Collate(child: Expression, collationName: String)
  extends UnaryExpression with ExpectsInputTypes {
  private val collationId = CollationFactory.collationNameToId(collationName)
  override def dataType: DataType = StringType(collationId)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

  override protected def withNewChildInternal(
    newChild: Expression): Expression = copy(newChild)

  override def eval(row: InternalRow): Any = child.eval(row)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (in) => in)
}

// scalastyle:off line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the collation name of a given expression.",
  arguments = """
    Arguments:
      * expr - String expression to perform collation on.
  """,
  examples = """
    Examples:
      > SET spark.sql.collation.enabled=true;
      spark.sql.collation.enabled	true
      > SELECT _FUNC_('Spark SQL');
      UTF8_BINARY
      > SET spark.sql.collation.enabled=false;
      spark.sql.collation.enabled	false
  """,
  since = "4.0.0",
  group = "string_funcs")
// scalastyle:on line.contains.tab
case class Collation(child: Expression) extends UnaryExpression with RuntimeReplaceable {
  override def dataType: DataType = StringType
  override protected def withNewChildInternal(newChild: Expression): Collation = copy(newChild)
  override def replacement: Expression = {
    val collationId = child.dataType.asInstanceOf[StringType].collationId
    val collationName = CollationFactory.fetchCollation(collationId).collationName
    Literal.create(collationName, StringType)
  }
}
