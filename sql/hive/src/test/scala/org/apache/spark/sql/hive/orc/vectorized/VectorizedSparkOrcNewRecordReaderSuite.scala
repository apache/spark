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

package org.apache.spark.sql.hive.orc.vectorized

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.sql.Timestamp

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.{OrcStruct, SparkOrcNewRecordReader, VectorizedSparkOrcNewRecordReader}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.datasources.{LogicalRelation, RecordReaderIterator}
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.hive.{HiveUtils, MetastoreRelation}
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class VectorizedSparkOrcNewRecordReaderSuite extends QueryTest with BeforeAndAfterAll with OrcTest {
  val key = SQLConf.ORC_VECTORIZED_READER_ENABLED.key
  val value = "true"
  private var currentValue: Option[String] = None

  override protected def beforeAll(): Unit = {
    currentValue = Try(spark.conf.get(key)).toOption
    spark.conf.set(key, value)
  }

  override protected def afterAll(): Unit = {
    currentValue match {
      case Some(value) => spark.conf.set(key, value)
      case None => spark.conf.unset(key)
    }
  }

  private def getVectorizedOrcReader(
      filepath: String,
      requiredSchema: StructType,
      partitionSchema: StructType,
      partitionValues: InternalRow): VectorizedSparkOrcNewRecordReader = {
    val conf = new Configuration()
    val physicalSchema = OrcFileOperator.readSchema(Seq(filepath), Some(conf)).get
    OrcRelation.setRequiredColumns(conf, physicalSchema, requiredSchema)
    val orcReader = OrcFileOperator.getFileReader(filepath, Some(conf)).get

    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)

    val file = new File(filepath)
    val fileSplit = new FileSplit(new Path(new URI(filepath)), 0, file.length(), Array.empty)
    val columnIDs =
      requiredSchema.map(a => physicalSchema.fieldIndex(a.name): Integer).sorted.asJava
    val orcRecordReader =
      new VectorizedSparkOrcNewRecordReader(
        orcReader, conf, fileSplit, columnIDs, requiredSchema, partitionSchema, partitionValues)

    val returningBatch: Boolean = OrcRelation.supportBatch(spark, resultSchema)
    if (returningBatch) {
      orcRecordReader.enableReturningBatches()
    }
    orcRecordReader
  }

  test("Read/write types: batch processing") {
    val data = (0 to 255).map { i =>
      (s"$i", i, i.toLong, i.toFloat, i.toDouble, i.toShort, i.toByte, i % 2 == 0,
        s"$i".getBytes(StandardCharsets.UTF_8))
    }
    val dataRows = data.map { x =>
      InternalRow(UTF8String.fromString(x._1), x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)
    }

    withOrcFile(data) { file =>
      val requiredSchema = new StructType()
        .add("_1", StringType)
        .add("_2", IntegerType)
        .add("_3", LongType)
        .add("_4", FloatType)
        .add("_5", DoubleType)
        .add("_6", ShortType)
        .add("_7", ByteType)
        .add("_8", BooleanType)
        .add("_9", BinaryType)
      val partitionSchema = StructType(Nil)
      val partitionValues = InternalRow.empty
      val reader = getVectorizedOrcReader(file, requiredSchema, partitionSchema, partitionValues)
      assert(reader.nextKeyValue())

      // The schema is supported by ColumnarBatch.
      val nextValue = reader.getCurrentValue()
      assert(nextValue.isInstanceOf[ColumnarBatch])

      val batch = nextValue.asInstanceOf[ColumnarBatch]

      assert(batch.numCols() == 9)
      assert(batch.numRows() == 256)
      assert(batch.numValidRows() == 256)
      assert(batch.capacity() > 0)
      assert(batch.rowIterator().hasNext == true)

      assert(batch.column(0).getUTF8String(0).toString() == "0")
      assert(batch.column(0).isNullAt(0) == false)
      assert(batch.column(1).getInt(0) == 0)
      assert(batch.column(1).isNullAt(0) == false)
      assert(batch.column(4).getDouble(0) == 0.0)
      assert(batch.column(4).isNullAt(0) == false)

      val it = batch.rowIterator()
      dataRows.map { row =>
        assert(it.hasNext())
        assert(it.next().copy() == row)
      }
    }
  }

  test("Read/write types: no batch processing") {
    val colNum = spark.conf.get(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key).toInt + 1
    val data = (0 to 255).map { i =>
      Row.fromSeq((i to colNum + i - 1).toSeq)
    }

    withTempPath { file =>
      val fields = (1 to colNum).map { idx =>
        StructField(s"_$idx", IntegerType)
      }
      val requiredSchema = StructType(fields.toArray)
      spark.createDataFrame(sparkContext.parallelize(data), requiredSchema)
        .write.orc(file.getCanonicalPath)
      val path = file.getCanonicalPath

      val partitionSchema = StructType(Nil)
      val partitionValues = InternalRow.empty
      val reader = getVectorizedOrcReader(path, requiredSchema, partitionSchema, partitionValues)
      assert(reader.nextKeyValue())

      // Column number exceeds SQLConf.WHOLESTAGE_MAX_NUM_FIELDS,
      // so batch processing is not supported.
      val nextValue = reader.getCurrentValue()
      assert(nextValue.isInstanceOf[ColumnarBatch.Row])

      val batchRow = nextValue.asInstanceOf[ColumnarBatch.Row]

      assert(batchRow.numFields() == colNum)

      var idx = 0
      while (reader.nextKeyValue()) {
        val row = data(idx)
        val batchRow = reader.getCurrentValue().asInstanceOf[ColumnarBatch.Row].copy()
        assert(batchRow.toSeq(requiredSchema) === row.toSeq)
        idx += 1
      }
    }
  }
}
