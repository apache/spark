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

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.TypeDescription

import org.apache.spark.TestUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.types.UTF8String.fromString

class OrcColumnarBatchReaderSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

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

  test("SPARK-33593: partition column types") {
    withTempPath { dir =>
      Seq(1).toDF().repartition(1).write.orc(dir.getCanonicalPath)

      val dataTypes =
        Seq(StringType, BooleanType, ByteType, BinaryType, ShortType, IntegerType, LongType,
          FloatType, DoubleType, DecimalType(25, 5), DateType, TimestampType)

      val constantValues =
        Seq(
          UTF8String.fromString("a string"),
          true,
          1.toByte,
          "Spark SQL".getBytes,
          2.toShort,
          3,
          Long.MaxValue,
          0.25.toFloat,
          0.75D,
          Decimal("1234.23456"),
          DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2015-01-01")),
          DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf("2015-01-01 23:50:59.123")))

      dataTypes.zip(constantValues).foreach { case (dt, v) =>
        val schema = StructType(StructField("col1", IntegerType) :: StructField("pcol", dt) :: Nil)
        val partitionValues = new GenericInternalRow(Array(v))
        val file = new File(TestUtils.listDirectory(dir).head)
        val fileSplit = new FileSplit(new Path(file.getCanonicalPath), 0L, file.length, Array.empty)
        val taskConf = sqlContext.sessionState.newHadoopConf()
        val orcFileSchema = TypeDescription.fromString(schema.simpleString)
        val vectorizedReader = new OrcColumnarBatchReader(4096)
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

        try {
          vectorizedReader.initialize(fileSplit, taskAttemptContext)
          vectorizedReader.initBatch(
            orcFileSchema,
            schema.toArray,
            Array(0, -1),
            Array(-1, 0),
            partitionValues)
          vectorizedReader.nextKeyValue()
          val row = vectorizedReader.getCurrentValue.getRow(0)

          // Use `GenericMutableRow` by explicitly copying rather than `ColumnarBatch`
          // in order to use get(...) method which is not implemented in `ColumnarBatch`.
          val actual = row.copy().get(1, dt)
          val expected = v
          if (dt.isInstanceOf[BinaryType]) {
            assert(actual.asInstanceOf[Array[Byte]]
              sameElements expected.asInstanceOf[Array[Byte]])
          } else {
            assert(actual == expected)
          }
        } finally {
          vectorizedReader.close()
        }
      }
    }
  }
}
