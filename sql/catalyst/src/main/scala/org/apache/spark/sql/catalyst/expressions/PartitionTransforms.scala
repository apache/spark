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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLExpr
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * Base class for expressions that are converted to v2 partition transforms.
 *
 * Subclasses represent abstract transform functions with concrete implementations that are
 * determined by data source implementations. Because the concrete implementation is not known,
 * these expressions are [[Unevaluable]].
 *
 * These expressions are used to pass transformations from the DataFrame API:
 *
 * {{{
 *   df.writeTo("catalog.db.table").partitionedBy($"category", days($"timestamp")).create()
 * }}}
 */
abstract class PartitionTransformExpression extends Expression with Unevaluable
  with UnaryLike[Expression] {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any =
    throw new SparkException(
      errorClass = "PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY",
      messageParameters = Map("expression" -> toSQLExpr(this)),
      cause = null
    )

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new SparkException(
      errorClass = "PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY",
      messageParameters = Map("expression" -> toSQLExpr(this)),
      cause = null
    )
}
/**
 * Expression for the v2 partition transform years.
 */
case class Years(child: Expression) extends PartitionTransformExpression {
  override def dataType: DataType = IntegerType
  override protected def withNewChildInternal(newChild: Expression): Years = copy(child = newChild)
}

/**
 * Expression for the v2 partition transform months.
 */
case class Months(child: Expression) extends PartitionTransformExpression {
  override def dataType: DataType = IntegerType
  override protected def withNewChildInternal(newChild: Expression): Months = copy(child = newChild)
}

/**
 * Expression for the v2 partition transform days.
 */
case class Days(child: Expression) extends PartitionTransformExpression {
  override def dataType: DataType = IntegerType
  override protected def withNewChildInternal(newChild: Expression): Days = copy(child = newChild)
}

/**
 * Expression for the v2 partition transform hours.
 */
case class Hours(child: Expression) extends PartitionTransformExpression {
  override def dataType: DataType = IntegerType
  override protected def withNewChildInternal(newChild: Expression): Hours = copy(child = newChild)
}

/**
 * Expression for the v2 partition transform bucket.
 */
case class Bucket(numBuckets: Literal, child: Expression) extends PartitionTransformExpression {
  def this(numBuckets: Expression, child: Expression) =
    this(Bucket.expressionToNumBuckets(numBuckets, child), child)

  override def dataType: DataType = IntegerType
  override protected def withNewChildInternal(newChild: Expression): Bucket = copy(child = newChild)
}

private[sql] object Bucket {
  def expressionToNumBuckets(numBuckets: Expression, e: Expression): Literal = numBuckets match {
    case l @ Literal(_, IntegerType) => l
    case _ =>
      throw QueryCompilationErrors.invalidBucketsNumberError(numBuckets.toString, e.toString)
  }
}
