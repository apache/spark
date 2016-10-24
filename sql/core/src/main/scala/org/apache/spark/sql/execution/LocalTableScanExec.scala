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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution.metric.SQLMetrics


/**
 * Physical plan node for scanning data from a local collection.
 */
case class LocalTableScanExec(
    output: Seq[Attribute],
    rows: Seq[InternalRow]) extends LeafExecNode with CodegenSupport {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(rdd)

  private val unsafeRows: Array[InternalRow] = {
    if (rows.isEmpty) {
      Array.empty
    } else {
      val proj = UnsafeProjection.create(output, output)
      rows.map(r => proj(r).copy()).toArray
    }
  }

  private lazy val numParallelism: Int = math.min(math.max(unsafeRows.length, 1),
    sqlContext.sparkContext.defaultParallelism)

  private lazy val rdd = sqlContext.sparkContext.parallelize(unsafeRows, numParallelism)

  protected override def doProduce(ctx: CodegenContext): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")
    val input = ctx.freshName("input")
    // Right now, LocalTableScanExec is only used when there is one upstream.
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val row = ctx.freshName("row")
    s"""
       | while ($input.hasNext()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   $numOutput.add(1);
       |   ${consume(ctx, null, row).trim}
       |   if (shouldStop()) return;
       | }
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.map { r =>
      numOutputRows += 1
      r
    }
  }

  override protected def stringArgs: Iterator[Any] = {
    if (rows.isEmpty) {
      Iterator("<empty>", output)
    } else {
      Iterator(output)
    }
  }

  override def executeCollect(): Array[InternalRow] = {
    longMetric("numOutputRows").add(unsafeRows.size)
    unsafeRows
  }

  override def executeTake(limit: Int): Array[InternalRow] = {
    val taken = unsafeRows.take(limit)
    longMetric("numOutputRows").add(taken.size)
    taken
  }
}
