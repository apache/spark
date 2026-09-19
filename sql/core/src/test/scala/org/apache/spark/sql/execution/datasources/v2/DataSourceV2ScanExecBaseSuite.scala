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

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.connector.read.{Batch, HasPartitionKey, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

class DataSourceV2ScanExecBaseSuite extends QueryTest with SharedSparkSession {

  private val exprA = AttributeReference("a", IntegerType)()
  private val exprB = AttributeReference("b", IntegerType)()

  test("SPARK-59642: a reported key of the wrong arity is rejected before the keys are sorted") {
    // The keys tie on `a`, so the sort that follows has to read `b` off the one-field key: were the
    // check to run after it, this would be an ArrayIndexOutOfBoundsException from the ordering.
    val exec = BatchScanExec(
      output = Seq(exprA, exprB),
      scan = new KeyedPartitionsScan(Seq(InternalRow(1, 5), InternalRow(1))),
      runtimeFilters = Seq.empty,
      table = null,
      keyGroupedPartitioning = Some(Seq(exprA, exprB)))

    withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "true") {
      val e = intercept[SparkException](exec.outputPartitioning)
      assert(e.getMessage.contains("partition key with 1 field(s)"))
      assert(e.getMessage.contains("2 partition expression(s)"))
    }
  }
}

private case class KeyedPartition(key: InternalRow) extends InputPartition with HasPartitionKey {
  override def partitionKey(): InternalRow = key
}

private class KeyedPartitionsScan(keys: Seq[InternalRow]) extends Scan with Batch {
  override def readSchema(): StructType =
    new StructType().add("a", IntegerType).add("b", IntegerType)

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = keys.map(KeyedPartition(_)).toArray

  override def createReaderFactory(): PartitionReaderFactory =
    throw new UnsupportedOperationException()
}
