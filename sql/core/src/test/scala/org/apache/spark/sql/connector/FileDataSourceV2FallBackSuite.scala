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
package org.apache.spark.sql.connector

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, QueryExecutionListener}

class DummyReadOnlyFileDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new DummyReadOnlyFileTable
  }
}

class DummyReadOnlyFileTable extends Table with SupportsRead {
  override def name(): String = "dummy"

  override def schema(): StructType = StructType(Nil)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    throw new AnalysisException("Dummy file reader")
  }

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.ACCEPT_ANY_SCHEMA)
}

class DummyWriteOnlyFileDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new DummyWriteOnlyFileTable
  }
}

class DummyWriteOnlyFileTable extends Table with SupportsWrite {
  override def name(): String = "dummy"

  override def schema(): StructType = StructType(Nil)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw new AnalysisException("Dummy file writer")

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_WRITE, TableCapability.ACCEPT_ANY_SCHEMA)
}

class FileDataSourceV2FallBackSuite extends QueryTest with SharedSparkSession {

  private val dummyReadOnlyFileSourceV2 = classOf[DummyReadOnlyFileDataSourceV2].getName
  private val dummyWriteOnlyFileSourceV2 = classOf[DummyWriteOnlyFileDataSourceV2].getName

  override protected def sparkConf: SparkConf = super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("Fall back to v1 when writing to file with read only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      // Writing file should fall back to v1 and succeed.
      df.write.format(dummyReadOnlyFileSourceV2).save(path)

      // Validate write result with [[ParquetFileFormat]].
      checkAnswer(spark.read.parquet(path), df)

      // Dummy File reader should fail as expected.
      val exception = intercept[AnalysisException] {
        spark.read.format(dummyReadOnlyFileSourceV2).load(path).collect()
      }
      assert(exception.message.equals("Dummy file reader"))
    }
  }

  test("Fall back read path to v1 with configuration USE_V1_SOURCE_LIST") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      df.write.parquet(path)
      Seq(
        "foo,parquet,bar",
        "ParQuet,bar,foo",
        s"foobar,$dummyReadOnlyFileSourceV2"
      ).foreach { fallbackReaders =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> fallbackReaders) {
          // Reading file should fall back to v1 and succeed.
          checkAnswer(spark.read.format(dummyReadOnlyFileSourceV2).load(path), df)
          checkAnswer(sql(s"SELECT * FROM parquet.`$path`"), df)
        }
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "foo,bar") {
        // Dummy File reader should fail as DISABLED_V2_FILE_DATA_SOURCE_READERS doesn't include it.
        val exception = intercept[AnalysisException] {
          spark.read.format(dummyReadOnlyFileSourceV2).load(path).collect()
        }
        assert(exception.message.equals("Dummy file reader"))
      }
    }
  }

  test("Fall back to v1 when reading file with write only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      df.write.parquet(path)
      // Fallback reads to V1
      checkAnswer(spark.read.format(dummyWriteOnlyFileSourceV2).load(path), df)
    }
  }

  test("Always fall back write path to v1") {
    val df = spark.range(10).toDF()
    withTempPath { path =>
      // Writes should fall back to v1 and succeed.
      df.write.format(dummyWriteOnlyFileSourceV2).save(path.getCanonicalPath)
      checkAnswer(spark.read.parquet(path.getCanonicalPath), df)
    }
  }

  test("Fallback Parquet V2 to V1") {
    Seq("parquet", classOf[ParquetDataSourceV2].getCanonicalName).foreach { format =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> format) {
        val commands = ArrayBuffer.empty[(String, LogicalPlan)]
        val exceptions = ArrayBuffer.empty[(String, Exception)]
        val listener = new QueryExecutionListener {
          override def onFailure(
              funcName: String,
              qe: QueryExecution,
              exception: Exception): Unit = {
            exceptions += funcName -> exception
          }

          override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
            commands += funcName -> qe.logical
          }
        }
        spark.listenerManager.register(listener)

        try {
          withTempPath { path =>
            val inputData = spark.range(10)
            inputData.write.format(format).save(path.getCanonicalPath)
            sparkContext.listenerBus.waitUntilEmpty()
            assert(commands.length == 1)
            assert(commands.head._1 == "command")
            assert(commands.head._2.isInstanceOf[InsertIntoHadoopFsRelationCommand])
            assert(commands.head._2.asInstanceOf[InsertIntoHadoopFsRelationCommand]
              .fileFormat.isInstanceOf[ParquetFileFormat])
            val df = spark.read.format(format).load(path.getCanonicalPath)
            checkAnswer(df, inputData.toDF())
            assert(
              df.queryExecution.executedPlan.exists(_.isInstanceOf[FileSourceScanExec]))
          }
        } finally {
          spark.listenerManager.unregister(listener)
        }
      }
    }
  }
}
