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
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * Expression that returns the current partition id.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current partition id.")
case class SparkPartitionID() extends LeafExpression with Nondeterministic {

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  @transient private[this] var partitionId: Int = _

  override val prettyName = "SPARK_PARTITION_ID"

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    partitionId = partitionIndex
  }

  override protected def evalInternal(input: InternalRow): Int = partitionId

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val idTerm = "partitionId"
    ctx.addImmutableStateIfNotExists(CodeGenerator.JAVA_INT, idTerm)
    ctx.addPartitionInitializationStatement(s"$idTerm = partitionIndex;")
    ev.copy(code = code"final ${CodeGenerator.javaType(dataType)} ${ev.value} = $idTerm;",
      isNull = FalseLiteral)
  }
}
