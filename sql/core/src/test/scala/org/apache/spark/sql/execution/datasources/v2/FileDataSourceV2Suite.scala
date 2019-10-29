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

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FileDataSourceV2Suite extends QueryTest with SharedSparkSession {

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
      val provider = new DummyFileSourceV2()
      val table = DataSourceV2Utils.getTableFromProvider(
        provider, options, userSpecifiedSchema = Some(expectedDataSchema))
        .asInstanceOf[DummyFileTable]
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
      val userSpecifiedSchema = StructType(Seq(
        StructField("v", StringType, true),
        StructField("p", IntegerType, true)))
      val expectedDataSchema = StructType(Seq(StructField("v", StringType, true)))
      val provider = new DummyFileSourceV2()
      val table = DataSourceV2Utils.getTableFromProvider(
        provider, options, userSpecifiedSchema = Some(userSpecifiedSchema))
        .asInstanceOf[DummyFileTable]
      assert(table.dataSchema == expectedDataSchema)
    }
  }
}

class DummyFileSourceV2 extends FileDataSourceV2 {
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[TextFileFormat]

  override def shortName(): String = "dummy"

  override protected def inferDataSchema(
      files: Seq[FileStatus], options: Map[String, String]): Option[StructType] = {
    throw new UnsupportedOperationException
  }

  override protected def createFileTable(
      paths: Seq[String],
      fileIndexGetter: () => PartitioningAwareFileIndex,
      dataSchema: StructType,
      partitionSchema: StructType,
      tableProps: util.Map[String, String]): FileTable = {
    DummyFileTable(sparkSession, paths, fileIndexGetter, dataSchema, partitionSchema, tableProps)
  }
}

case class DummyFileTable(
    sparkSession: SparkSession,
    paths: Seq[String],
    fileIndexGetter: () => PartitioningAwareFileIndex,
    dataSchema: StructType,
    partitionSchema: StructType,
    override val properties: java.util.Map[String, String]) extends FileTable {
  override def formatName: String = "Dummy"

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = null

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = null

  override def supportsDataType(dataType: DataType): Boolean = dataType == StringType

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[TextFileFormat]
}
