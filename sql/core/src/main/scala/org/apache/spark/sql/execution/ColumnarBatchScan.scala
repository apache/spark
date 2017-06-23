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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.types.DataType


/**
 * Helper trait for abstracting scan functionality using
 * [[org.apache.spark.sql.execution.vectorized.ColumnarBatch]]es.
 */
private[sql] trait ColumnarBatchScan extends CodegenSupport {

  val inMemoryTableScan: InMemoryTableScanExec = null

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))

  /**
   * Generate [[ColumnVector]] expressions for our parent to consume as rows.
   * This is called once per [[ColumnarBatch]].
   */
  private def genCodeColumnVector(
      ctx: CodegenContext,
      columnVar: String,
      ordinal: String,
      dataType: DataType,
      nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val value = ctx.getValue(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) { ctx.freshName("isNull") } else { "false" }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = s"${ctx.registerComment(str)}\n" + (if (nullable) {
      s"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $javaType $valueVar = $isNullVar ? ${ctx.defaultValue(dataType)} : ($value);
      """
    } else {
      s"$javaType $valueVar = $value;"
    }).trim
    ExprCode(code, isNullVar, valueVar)
  }

  /**
   * Produce code to process the input iterator as [[ColumnarBatch]]es.
   * This produces an [[UnsafeRow]] for each row in each batch.
   */
  // TODO: return ColumnarBatch.Rows instead
  override protected def doProduce(ctx: CodegenContext): String = {
    val input = ctx.freshName("input")
    // PhysicalRDD always just has one input
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")

    // metrics
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val scanTimeMetric = metricTerm(ctx, "scanTime")
    val scanTimeTotalNs = ctx.freshName("scanTime")
    ctx.addMutableState("long", scanTimeTotalNs, s"$scanTimeTotalNs = 0;")

    val columnarBatchClz = "org.apache.spark.sql.execution.vectorized.ColumnarBatch"
    val batch = ctx.freshName("batch")
    ctx.addMutableState(columnarBatchClz, batch, s"$batch = null;")

    val columnVectorClz = "org.apache.spark.sql.execution.vectorized.ColumnVector"
    val idx = ctx.freshName("batchIdx")
    ctx.addMutableState("int", idx, s"$idx = 0;")
    val colVars = output.indices.map(i => ctx.freshName("colInstance" + i))
    val columnAssigns = colVars.zipWithIndex.map { case (name, i) =>
      ctx.addMutableState(columnVectorClz, name, s"$name = null;")
      s"$name = $batch.column($i);"
    }

    val nextBatch = ctx.freshName("nextBatch")
    ctx.addNewFunction(nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  long getBatchStart = System.nanoTime();
         |  if ($input.hasNext()) {
         |    $batch = ($columnarBatchClz)$input.next();
         |    $numOutputRows.add($batch.numRows());
         |    $idx = 0;
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |  }
         |  $scanTimeTotalNs += System.nanoTime() - getBatchStart;
         |}""".stripMargin)

    ctx.currentVars = null
    val rowidx = ctx.freshName("rowIdx")
    val columnsBatchInput = (output zip colVars).map { case (attr, colVar) =>
      genCodeColumnVector(ctx, colVar, rowidx, attr.dataType, attr.nullable)
    }
    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val numRows = ctx.freshName("numRows")
    val shouldStop = if (isShouldStopRequired) {
      s"if (shouldStop()) { $idx = $rowidx + 1; return; }"
    } else {
      "// shouldStop check is eliminated"
    }
    s"""
       |if ($batch == null) {
       |  $nextBatch();
       |}
       |while ($batch != null) {
       |  int $numRows = $batch.numRows();
       |  int $localEnd = $numRows - $idx;
       |  for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
       |    int $rowidx = $idx + $localIdx;
       |    ${consume(ctx, columnsBatchInput).trim}
       |    $shouldStop
       |  }
       |  $idx = $numRows;
       |  $batch = null;
       |  $nextBatch();
       |}
       |$scanTimeMetric.add($scanTimeTotalNs / (1000 * 1000));
       |$scanTimeTotalNs = 0;
     """.stripMargin
  }

}
