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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{Footer, ParquetFileReader, ParquetFileWriter, ParquetOutputFormat}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType

import org.apache.spark.TestUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.FileBasedDataSourceTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * A helper trait that provides convenient facilities for Parquet testing.
 *
 * NOTE: Considering classes `Tuple1` ... `Tuple22` all extend `Product`, it would be more
 * convenient to use tuples rather than special case classes when writing test cases/suites.
 * Especially, `Tuple1.apply` can be used to easily wrap a single type/value.
 */
private[sql] trait ParquetTest extends FileBasedDataSourceTest {

  override protected val dataSourceName: String = "parquet"
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key
  override protected val vectorizedReaderNestedEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key

  /**
   * Reads the parquet file at `path`
   */
  protected def readParquetFile(path: String, testVectorized: Boolean = true)
      (f: DataFrame => Unit) = readFile(path, testVectorized)(f)

  /**
   * Writes `data` to a Parquet file, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withParquetFile[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: String => Unit): Unit = withDataSourceFile(data)(f)

  /**
   * Writes `data` to a Parquet file and reads it back as a [[DataFrame]],
   * which is then passed to `f`. The Parquet file will be deleted after `f` returns.
   */
  protected def withParquetDataFrame[T <: Product: ClassTag: TypeTag]
      (data: Seq[T], testVectorized: Boolean = true)
      (f: DataFrame => Unit): Unit = withDataSourceDataFrame(data, testVectorized)(f)

  /**
   * Writes `data` to a Parquet file, reads it back as a [[DataFrame]] and registers it as a
   * temporary table named `tableName`, then call `f`. The temporary table together with the
   * Parquet file will be dropped/deleted after `f` returns.
   */
  protected def withParquetTable[T <: Product: ClassTag: TypeTag]
      (data: Seq[T], tableName: String, testVectorized: Boolean = true)
      (f: => Unit): Unit = withDataSourceTable(data, tableName, testVectorized)(f)

  protected def makeParquetFile[T <: Product: ClassTag: TypeTag](
      data: Seq[T], path: File): Unit = makeDataSourceFile(data, path)

  protected def makeParquetFile[T <: Product: ClassTag: TypeTag](
      df: DataFrame, path: File): Unit = makeDataSourceFile(df, path)

  protected def makePartitionDir(
      basePath: File,
      defaultPartitionName: String,
      partitionCols: (String, Any)*): File = {
    val partNames = partitionCols.map { case (k, v) =>
      val valueString = if (v == null || v == "") defaultPartitionName else v.toString
      s"$k=$valueString"
    }

    val partDir = partNames.foldLeft(basePath) { (parent, child) =>
      new File(parent, child)
    }

    assert(partDir.mkdirs(), s"Couldn't create directory $partDir")
    partDir
  }

  protected def writeMetadata(
      schema: StructType, path: Path, configuration: Configuration): Unit = {
    val parquetSchema = new SparkToParquetSchemaConverter().convert(schema)
    val extraMetadata = Map(ParquetReadSupport.SPARK_METADATA_KEY -> schema.json).asJava
    val createdBy = s"Apache Spark ${org.apache.spark.SPARK_VERSION}"
    val fileMetadata = new FileMetaData(parquetSchema, extraMetadata, createdBy)
    val parquetMetadata = new ParquetMetadata(fileMetadata, Seq.empty[BlockMetaData].asJava)
    val footer = new Footer(path, parquetMetadata)
    ParquetFileWriter.writeMetadataFile(configuration, path, Seq(footer).asJava)
  }

  /**
   * This is an overloaded version of `writeMetadata` above to allow writing customized
   * Parquet schema.
   */
  protected def writeMetadata(
      parquetSchema: MessageType, path: Path, configuration: Configuration,
      extraMetadata: Map[String, String] = Map.empty[String, String]): Unit = {
    val extraMetadataAsJava = extraMetadata.asJava
    val createdBy = s"Apache Spark ${org.apache.spark.SPARK_VERSION}"
    val fileMetadata = new FileMetaData(parquetSchema, extraMetadataAsJava, createdBy)
    val parquetMetadata = new ParquetMetadata(fileMetadata, Seq.empty[BlockMetaData].asJava)
    val footer = new Footer(path, parquetMetadata)
    ParquetFileWriter.writeMetadataFile(configuration, path, Seq(footer).asJava)
  }

  protected def readAllFootersWithoutSummaryFiles(
      path: Path, configuration: Configuration): Seq[Footer] = {
    val fs = path.getFileSystem(configuration)
    ParquetFileReader.readAllFootersInParallel(configuration, fs.getFileStatus(path)).asScala.toSeq
  }

  protected def readFooter(path: Path, configuration: Configuration): ParquetMetadata = {
    ParquetFooterReader.readFooter(
      configuration,
      new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE),
      ParquetMetadataConverter.NO_FILTER)
  }

  protected def testStandardAndLegacyModes(testName: String)(f: => Unit): Unit = {
    test(s"Standard mode - $testName") {
      withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false") { f }
    }

    test(s"Legacy mode - $testName") {
      withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "true") { f }
    }
  }

  protected def readResourceParquetFile(name: String): DataFrame = {
    spark.read.parquet(getResourceParquetFilePath(name))
  }

  protected def getResourceParquetFilePath(name: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(name).toString
  }

  def withAllParquetReaders(code: => Unit): Unit = {
    // test the row-based reader
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
      withClue("Parquet-mr reader") {
        code
      }
    }
    // test the vectorized reader
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      withClue("Vectorized reader") {
        code
      }
    }
  }

  def withAllParquetWriters(code: => Unit): Unit = {
    // Parquet version 1
    withSQLConf(ParquetOutputFormat.WRITER_VERSION ->
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)(code)
    // Parquet version 2
    withSQLConf(ParquetOutputFormat.WRITER_VERSION ->
      ParquetProperties.WriterVersion.PARQUET_2_0.toString)(code)
  }

  def getMetaData(dir: java.io.File): Map[String, String] = {
    val file = TestUtils.listDirectory(dir).head
    val conf = new Configuration()
    val hadoopInputFile = HadoopInputFile.fromPath(new Path(file), conf)
    val parquetReadOptions = HadoopReadOptions.builder(conf).build()
    val m = ParquetFileReader.open(hadoopInputFile, parquetReadOptions)
    val metadata = try {
      m.getFileMetaData.getKeyValueMetaData
    } finally {
      m.close()
    }
    metadata.asScala.toMap
  }
}
