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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{ColumnarBatchScan, LeafExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, PartitionReaderFactory, Scan, SupportsReportPartitioning}

trait DataSourceV2ScanExecBase extends LeafExecNode with ColumnarBatchScan {

  def scan: Scan

  def partitions: Seq[InputPartition]

  def readerFactory: PartitionReaderFactory

  override def simpleString(maxFields: Int): String = {
    s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)} ${scan.description()}"
  }

  override def outputPartitioning: physical.Partitioning = scan match {
    case _ if partitions.length == 1 =>
      SinglePartition

    case s: SupportsReportPartitioning =>
      new DataSourcePartitioning(
        s.outputPartitioning(), AttributeMap(output.map(a => a -> a.name)))

    case _ => super.outputPartitioning
  }

  override def supportsBatch: Boolean = {
    require(partitions.forall(readerFactory.supportColumnarReads) ||
      !partitions.exists(readerFactory.supportColumnarReads),
      "Cannot mix row-based and columnar input partitions.")

    partitions.exists(readerFactory.supportColumnarReads)
  }

  def inputRDD: RDD[InternalRow]

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      val numOutputRows = longMetric("numOutputRows")
      inputRDD.map { r =>
        numOutputRows += 1
        r
      }
    }
  }
}
