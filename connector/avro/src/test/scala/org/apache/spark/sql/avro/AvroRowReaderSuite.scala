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

package org.apache.spark.sql.avro

import java.io._
import java.net.URI

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.LegacyBehaviorPolicy._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.avro.AvroScan

class AvroRowReaderSuite
  extends QueryTest
  with SharedSparkSession {

  import testImplicits._

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "") // need this for BatchScanExec

  test("SPARK-33314: hasNextRow and nextRow properly handle consecutive calls") {
    withTempPath { dir =>
      Seq((1), (2), (3))
        .toDF("value")
        .coalesce(1)
        .write
        .format("avro")
        .save(dir.getCanonicalPath)

      val df = spark.read.format("avro").load(dir.getCanonicalPath)
      val fileScan = df.queryExecution.executedPlan collectFirst {
        case BatchScanExec(_, f: AvroScan, _, _, _, _) => f
      }
      val filePath = fileScan.get.fileIndex.inputFiles(0)
      val fileSize = new File(new URI(filePath)).length
      // scalastyle:off pathfromuri
      val in = new FsInput(new Path(new URI(filePath)), new Configuration())
      // scalastyle:on pathfromuri
      val reader = DataFileReader.openReader(in, new GenericDatumReader[GenericRecord]())

      val it = new Iterator[InternalRow] with AvroUtils.RowReader {
        override val fileReader = reader
        override val deserializer = new AvroDeserializer(
          reader.getSchema,
          StructType(new StructField("value", IntegerType, true) :: Nil),
          false,
          RebaseSpec(CORRECTED),
          new NoopFilters,
          false)
        override val stopPosition = fileSize

        override def hasNext: Boolean = hasNextRow

        override def next: InternalRow = nextRow
      }
      assert(it.hasNext == true)
      assert(it.next.getInt(0) == 1)
      // test no intervening next
      assert(it.hasNext == true)
      assert(it.hasNext == true)
      // test no intervening hasNext
      assert(it.next.getInt(0) == 2)
      assert(it.next.getInt(0) == 3)
      assert(it.hasNext == false)
      assertThrows[NoSuchElementException] {
        it.next
      }
    }
  }
}
