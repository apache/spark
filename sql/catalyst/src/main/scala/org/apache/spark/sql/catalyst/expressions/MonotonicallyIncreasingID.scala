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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{DataType, LongType}

/**
 * Returns monotonically increasing 64-bit integers.
 *
 * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
 * The current implementation puts the partition ID in the upper 31 bits, and the lower 33 bits
 * represent the record number within each partition. The assumption is that the data frame has
 * less than 1 billion partitions, and each partition has less than 8 billion records.
 *
 * Since this expression is stateful, it cannot be a case object.
 */
@ExpressionDescription(
  usage = """
    _FUNC_() - Returns monotonically increasing 64-bit integers. The generated ID is guaranteed
      to be monotonically increasing and unique, but not consecutive. The current implementation
      puts the partition ID in the upper 31 bits, and the lower 33 bits represent the record number
      within each partition. The assumption is that the data frame has less than 1 billion
      partitions, and each partition has less than 8 billion records.
      The function is non-deterministic because its result depends on partition IDs.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_();
       0
  """,
  since = "1.4.0",
  group = "misc_funcs")
case class MonotonicallyIncreasingID() extends LeafExpression with Nondeterministic {

  /**
   * Record ID within each partition. By being transient, count's value is reset to 0 every time
   * we serialize and deserialize and initialize it.
   */
  @transient private[this] var count: Long = _

  @transient private[this] var partitionMask: Long = _

  override def stateful: Boolean = true

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    count = 0L
    partitionMask = partitionIndex.toLong << 33
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    MonotonicallyIncreasingID()
  }

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override protected def evalInternal(input: InternalRow): Long = {
    val currentCount = count
    count += 1
    partitionMask + currentCount
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val countTerm = ctx.addMutableState(CodeGenerator.JAVA_LONG, "count")
    val partitionMaskTerm = "partitionMask"
    ctx.addImmutableStateIfNotExists(CodeGenerator.JAVA_LONG, partitionMaskTerm)
    ctx.addPartitionInitializationStatement(s"$countTerm = 0L;")
    ctx.addPartitionInitializationStatement(s"$partitionMaskTerm = ((long) partitionIndex) << 33;")

    ev.copy(code = code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = $partitionMaskTerm + $countTerm;
      $countTerm++;""", isNull = FalseLiteral)
  }

  override def nodeName: String = "monotonically_increasing_id"

  override def sql: String = s"$prettyName()"
}
