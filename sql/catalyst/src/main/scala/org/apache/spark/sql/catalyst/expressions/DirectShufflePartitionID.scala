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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{DataType, IntegerType, IntegralType}

/**
 * Expression that takes a partition ID value and passes it through directly for use in
 * shuffle partitioning. This is used with RepartitionByExpression to allow users to
 * directly specify target partition IDs instead of using hash-based partitioning.
 *
 * The child expression must evaluate to an integral type and must not be null.
 * The resulting partition ID must be in the range [0, numPartitions).
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the partition ID specified by expr for direct shuffle " +
    "partitioning.",
  arguments = """
    Arguments:
      * expr - an integral expression that specifies the target partition ID
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0);
       0
      > SELECT _FUNC_(col_name % 4);
       (varies based on col_name value)
  """,
  since = "4.0.0",
  group = "misc_funcs")
case class DirectShufflePartitionID(child: Expression) extends UnaryExpression {

  override def dataType: DataType = IntegerType

  override def nullable: Boolean = false

  override val prettyName: String = "direct_shuffle_partition_id"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType.isInstanceOf[IntegralType]) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName requires an integral type, but got ${child.dataType.catalogString}")
    }
  }

  override def nullSafeEval(input: Any): Any = {
    val partitionId = input.asInstanceOf[Number].intValue()
    if (partitionId < 0) {
      throw new IllegalArgumentException(
        s"The partition ID expression must be non-negative, but got: $partitionId")
    }
    partitionId
  }

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == null) {
      throw new IllegalArgumentException(
        "The partition ID expression must not be null.")
    }
    nullSafeEval(result)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val resultCode =
      s"""
         |${childGen.code}
         |if (${childGen.isNull}) {
         |  throw new IllegalArgumentException(
         |    "The partition ID expression must not be null.");
         |}
         |int ${ev.value} = (int) ${childGen.value};
         |if (${ev.value} < 0) {
         |  throw new IllegalArgumentException(
         |    "The partition ID expression must be non-negative, but got: " + ${ev.value});
         |}
         |""".stripMargin

    ev.copy(code = code"$resultCode", isNull = FalseLiteral)
  }

  override protected def withNewChildInternal(newChild: Expression): DirectShufflePartitionID =
    copy(child = newChild)
}
