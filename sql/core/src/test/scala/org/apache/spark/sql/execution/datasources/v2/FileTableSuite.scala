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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DummyFileTable(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    expectedDataSchema: StructType,
    userSpecifiedSchema: Option[StructType])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = Some(expectedDataSchema)

  override def name(): String = "Dummy"

  override def formatName: String = "Dummy"

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = null

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = null

  override def supportsDataType(dataType: DataType): Boolean = dataType == StringType

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[TextFileFormat]
}

class FileTableSuite extends QueryTest with SharedSparkSession {

  test("Data type validation should check data schema only") {
    withTempPath { dir =>
      val df = spark.createDataFrame(Seq(("a", 1), ("b", 2))).toDF("v", "p")
      val pathName = dir.getCanonicalPath
      df.write.partitionBy("p").text(pathName)
      val options = new CaseInsensitiveStringMap(Map("path" -> pathName).asJava)
      val expectedDataSchema = StructType(Seq(StructField("v", StringType, true)))
      // DummyFileTable doesn't support Integer data type.
      // However, the partition schema is handled by Spark, so it is allowed to contain
      // Integer data type here.
      val table = new DummyFileTable(spark, options, Seq(pathName), expectedDataSchema, None)
      assert(table.dataSchema == expectedDataSchema)
      val expectedPartitionSchema = StructType(Seq(StructField("p", IntegerType, true)))
      assert(table.fileIndex.partitionSchema == expectedPartitionSchema)
    }
  }

  test("Returns correct data schema when user specified schema contains partition schema") {
    withTempPath { dir =>
      val df = spark.createDataFrame(Seq(("a", 1), ("b", 2))).toDF("v", "p")
      val pathName = dir.getCanonicalPath
      df.write.partitionBy("p").text(pathName)
      val options = new CaseInsensitiveStringMap(Map("path" -> pathName).asJava)
      val userSpecifiedSchema = Some(StructType(Seq(
        StructField("v", StringType, true),
        StructField("p", IntegerType, true))))
      val expectedDataSchema = StructType(Seq(StructField("v", StringType, true)))
      val table =
        new DummyFileTable(spark, options, Seq(pathName), expectedDataSchema, userSpecifiedSchema)
      assert(table.dataSchema == expectedDataSchema)
    }
  }
}
