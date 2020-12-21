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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class OrcColumnarBatchReaderSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

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
        val conf = sqlContext.conf
        val dataSchema = StructType(StructField("col1", IntegerType) :: Nil)
        val partitionSchema = StructType(StructField("pcol", dt) :: Nil)
        val partitionValues = new GenericInternalRow(Array(v))
        val file = new File(SpecificParquetRecordReaderBase.listDirectory(dir).get(0))
        val fileSplit = new FileSplit(new Path(file.getCanonicalPath), 0L, file.length, Array.empty)
        val taskConf = sqlContext.sessionState.newHadoopConf()
        val orcFileSchema = TypeDescription.fromString(dataSchema.simpleString)
        val vectorizedReader = new OrcColumnarBatchReader(
          conf.offHeapColumnVectorEnabled, conf.getConf(SQLConf.ORC_COPY_BATCH_TO_SPARK), 4096)
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val taskAttemptContext = new TaskAttemptContextImpl(taskConf, attemptId)

        try {
          vectorizedReader.initialize(fileSplit, taskAttemptContext)
          vectorizedReader.initBatch(
            orcFileSchema,
            Array(0),
            dataSchema.toArray,
            partitionSchema,
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
