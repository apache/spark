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
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String.fromString

class OrcColumnarBatchReaderSuite extends QueryTest with SQLTestUtils with SharedSQLContext {
  private val dataSchema = StructType.fromDDL("col1 int, col2 int")
  private val partitionSchema = StructType.fromDDL("p1 string, p2 string")
  private val partitionValues = InternalRow(fromString("partValue1"), fromString("partValue2"))
  private val orcFileSchemaList = Seq(
    "struct<col1:int,col2:int>", "struct<col1:int,col2:int,p1:string,p2:string>",
    "struct<col1:int,col2:int,p1:string>", "struct<col1:int,col2:int,p2:string>")
  orcFileSchemaList.foreach { case schema =>
    val orcFileSchema = TypeDescription.fromString(schema)

    val isConstant = classOf[WritableColumnVector].getDeclaredField("isConstant")
    isConstant.setAccessible(true)

    def getReader(
        requestedDataColIds: Array[Int],
        requestedPartitionColIds: Array[Int],
        resultFields: Array[StructField]): OrcColumnarBatchReader = {
      val reader = new OrcColumnarBatchReader(4096)
      reader.initBatch(
        orcFileSchema,
        resultFields,
        requestedDataColIds,
        requestedPartitionColIds,
        partitionValues)
      reader
    }

    test(s"all partitions are requested: $schema") {
      val requestedDataColIds = Array(0, 1, 0, 0)
      val requestedPartitionColIds = Array(-1, -1, 0, 1)
      val reader = getReader(requestedDataColIds, requestedPartitionColIds,
        dataSchema.fields ++ partitionSchema.fields)
      assert(reader.requestedDataColIds === Array(0, 1, -1, -1))
    }

    test(s"initBatch should initialize requested partition columns only: $schema") {
      val requestedDataColIds = Array(0, -1) // only `col1` is requested, `col2` doesn't exist
      val requestedPartitionColIds = Array(-1, 0) // only `p1` is requested
      val reader = getReader(requestedDataColIds, requestedPartitionColIds,
        Array(dataSchema.fields(0), partitionSchema.fields(0)))
      val batch = reader.columnarBatch
      assert(batch.numCols() === 2)

      assert(batch.column(0).isInstanceOf[OrcColumnVector])
      assert(batch.column(1).isInstanceOf[OnHeapColumnVector])

      val p1 = batch.column(1).asInstanceOf[OnHeapColumnVector]
      assert(isConstant.get(p1).asInstanceOf[Boolean]) // Partition column is constant.
      assert(p1.getUTF8String(0) === partitionValues.getUTF8String(0))
    }
  }
}
