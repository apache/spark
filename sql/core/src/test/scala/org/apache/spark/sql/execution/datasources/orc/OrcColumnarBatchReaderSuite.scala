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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.orc.TypeDescription

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String.fromString

class OrcColumnarBatchReaderSuite extends QueryTest with SQLTestUtils with SharedSQLContext {

  private val orcFileSchema = TypeDescription.fromString(s"struct<col1:int,col2:int>")
  private val requiredSchema = StructType.fromDDL("col1 int, col3 int")
  private val partitionSchema = StructType.fromDDL("partCol1 string, partCol2 string")
  private val partitionValues = InternalRow(fromString("partValue1"), fromString("partValue2"))
  private val resultSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)

  private val isConstant = classOf[WritableColumnVector].getDeclaredField("isConstant")
  isConstant.setAccessible(true)

  private def getReader(requestedDataColIds: Array[Int], requestedPartitionColIds: Array[Int]) = {
    val reader = new OrcColumnarBatchReader(false, false, 4096)
    reader.initBatch(
      orcFileSchema,
      resultSchema.fields,
      requestedDataColIds,
      requestedPartitionColIds,
      partitionValues)
    reader
  }

  test("requestedPartitionColIds resets requestedDataColIds - all partitions are requested") {
    val requestedDataColIds = Array(0, 1, 0, 0)
    val requestedPartitionColIds = Array(-1, -1, 0, 1)
    val reader = getReader(requestedDataColIds, requestedPartitionColIds)
    assert(reader.requestedDataColIds === Array(0, 1, -1, -1))
  }

  test("requestedPartitionColIds resets requestedDataColIds - one partition is requested") {
    Seq((Array(-1, -1, 0, -1), Array(0, 1, -1, 0)),
      (Array(-1, -1, -1, 0), Array(0, 1, 0, -1))).foreach {
      case (requestedPartitionColIds, answer) =>
        val requestedDataColIds = Array(0, 1, 0, 0)
        val reader = getReader(requestedDataColIds, requestedPartitionColIds)
        assert(reader.requestedDataColIds === answer)
    }
  }

  test("initBatch should initialize requested partition columns only") {
    val requestedDataColIds = Array(0, -1, -1, -1) // only `col1` is requested, `col3` doesn't exist
    val requestedPartitionColIds = Array(-1, -1, 0, -1) // only `partCol1` is requested
    val reader = getReader(requestedDataColIds, requestedPartitionColIds)
    val batch = reader.columnarBatch
    assert(batch.numCols() === 4)

    assert(batch.column(0).isInstanceOf[OrcColumnVector])
    assert(batch.column(1).isInstanceOf[OnHeapColumnVector])
    assert(batch.column(2).isInstanceOf[OnHeapColumnVector])
    assert(batch.column(3).isInstanceOf[OnHeapColumnVector])

    val col3 = batch.column(1).asInstanceOf[OnHeapColumnVector]
    val partCol1 = batch.column(2).asInstanceOf[OnHeapColumnVector]
    val partCol2 = batch.column(3).asInstanceOf[OnHeapColumnVector]

    assert(isConstant.get(col3).asInstanceOf[Boolean]) // `col3` is NULL.
    assert(isConstant.get(partCol1).asInstanceOf[Boolean]) // Partition column is constant.
    assert(isConstant.get(partCol2).asInstanceOf[Boolean]) // Null column is constant.

    assert(partCol1.getUTF8String(0) === partitionValues.getUTF8String(0))
    assert(partCol2.isNullAt(0)) // This is NULL because it's not requested.
  }
}
