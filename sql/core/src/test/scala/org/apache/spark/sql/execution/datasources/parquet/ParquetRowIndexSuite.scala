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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties._
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat}
import org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.ArrayImplicits._

@SlowSQLTest
class ParquetRowIndexSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private def readRowGroupRowCounts(path: String): Seq[Long] = {
    ParquetFileReader.readFooter(spark.sessionState.newHadoopConf(), new Path(path))
      .getBlocks.asScala.toSeq.map(_.getRowCount)
  }

  private def readRowGroupRowCounts(dir: File): Seq[Seq[Long]] = {
    assert(dir.isDirectory)
    dir.listFiles()
      .filter { f => f.isFile && f.getName.endsWith("parquet") }
      .map { f => readRowGroupRowCounts(f.getAbsolutePath) }.toImmutableArraySeq
  }

  /**
   * Do the files contain exactly one row group?
   */
  private def assertOneRowGroup(dir: File): Unit = {
    readRowGroupRowCounts(dir).foreach { rcs =>
      assert(rcs.length == 1, "expected one row group per file")
    }
  }

  /**
   * Do the files have a good layout to test row group skipping (both range metadata filter, and
   * by using min/max).
   */
  private def assertTinyRowGroups(dir: File): Unit = {
    readRowGroupRowCounts(dir).foreach { rcs =>
      assert(rcs.length > 1, "expected multiple row groups per file")
      assert(rcs.last <= DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK)
      assert(rcs.reverse.tail.distinct == Seq(DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK),
        "expected row groups with minimal row count")
    }
  }

  /**
   * Do the files have a good layout to test a combination of page skipping and row group skipping?
   */
  private def assertIntermediateRowGroups(dir: File): Unit = {
    readRowGroupRowCounts(dir).foreach { rcs =>
      assert(rcs.length >= 3, "expected at least 3 row groups per file")
      rcs.reverse.tail.foreach { rc =>
        assert(rc > DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK,
          "expected row groups larger than minimal row count")
      }
    }
  }

  case class RowIndexTestConf(
      numRows: Long = 10000L,
      useMultipleFiles: Boolean = false,
      useVectorizedReader: Boolean = true,
      useSmallPages: Boolean = false,
      useSmallRowGroups: Boolean = false,
      useSmallSplits: Boolean = false,
      useFilter: Boolean = false,
      useDataSourceV2: Boolean = false) {

    val NUM_MULTIPLE_FILES = 4
    // The test doesn't work correctly if the number of records per file is uneven.
    assert(!useMultipleFiles || (numRows % NUM_MULTIPLE_FILES == 0))

    def numFiles: Int = if (useMultipleFiles) { NUM_MULTIPLE_FILES } else { 1 }

    def rowGroupSize: Long = if (useSmallRowGroups) {
      if (useSmallPages) {
        // Each file will contain multiple row groups. All of them (except for the last one)
        // will contain more than DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK, so that individual
        // pages within the row group can be skipped.
        2048L
      } else {
        // Each file will contain multiple row groups. All of them (except for the last one)
        // will contain exactly DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK records.
        64L
      }
    } else {
      // Each file will contain a single row group.
      DEFAULT_BLOCK_SIZE
    }

    def pageSize: Long = if (useSmallPages) {
      // Each page (except for the last one for each column) will contain exactly
      // DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK records.
      64L
    } else {
      DEFAULT_PAGE_SIZE
    }

    def writeFormat: String = "parquet"
    def readFormat: String = if (useDataSourceV2) {
      classOf[ParquetDataSourceV2].getCanonicalName
    } else {
      "parquet"
    }

    assert(useSmallRowGroups || !useSmallSplits)
    def filesMaxPartitionBytes: Long = if (useSmallSplits) {
      256L
    } else {
      SQLConf.FILES_MAX_PARTITION_BYTES.defaultValue.get
    }

    def desc: String = {
      { if (useVectorizedReader) Seq("vectorized reader") else Seq("parquet-mr reader") } ++
      { if (useMultipleFiles) Seq("many files") else Seq.empty[String] } ++
      { if (useFilter) Seq("filtered") else Seq.empty[String] } ++
      { if (useSmallPages) Seq("small pages") else Seq.empty[String] } ++
      { if (useSmallRowGroups) Seq("small row groups") else Seq.empty[String] } ++
      { if (useSmallSplits) Seq("small splits") else Seq.empty[String] } ++
      { if (useDataSourceV2) Seq("datasource v2") else Seq.empty[String] }
    }.mkString(", ")

    def sqlConfs: Seq[(String, String)] = Seq(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> useVectorizedReader.toString,
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> filesMaxPartitionBytes.toString
    ) ++ { if (useDataSourceV2) Seq(SQLConf.USE_V1_SOURCE_LIST.key -> "") else Seq.empty }
  }

  for (useVectorizedReader <- Seq(true, false))
  for (useDataSourceV2 <- Seq(true, false))
  for (useSmallRowGroups <- Seq(true, false))
  for (useSmallPages <- Seq(true, false))
  for (useFilter <- Seq(true, false))
  for (useSmallSplits <- Seq(useSmallRowGroups, false).distinct) {
    val conf = RowIndexTestConf(useVectorizedReader = useVectorizedReader,
      useDataSourceV2 = useDataSourceV2, useSmallRowGroups = useSmallRowGroups,
      useSmallPages = useSmallPages, useFilter = useFilter,
      useSmallSplits = useSmallSplits)
    testRowIndexGeneration("row index generation", conf)
  }

  private def testRowIndexGeneration(label: String, conf: RowIndexTestConf): Unit = {
    test (s"$label - ${conf.desc}") {
      withSQLConf(conf.sqlConfs: _*) {
        withTempPath { path =>
          // Read row index using _metadata.row_index if that is supported by the file format.
          val rowIndexMetadataColumnSupported = conf.readFormat match {
            case "parquet" => true
            case _ => false
          }
          val rowIndexColName = if (rowIndexMetadataColumnSupported) {
            s"${FileFormat.METADATA_NAME}.${ParquetFileFormat.ROW_INDEX}"
          } else {
            ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
          }
          val numRecordsPerFile = conf.numRows / conf.numFiles
          val (skipCentileFirst, skipCentileMidLeft, skipCentileMidRight, skipCentileLast) =
            (0.2, 0.4, 0.6, 0.8)
          val expectedRowIdxCol = "expected_rowIdx_col"
          val df = spark.range(0, conf.numRows, 1, conf.numFiles).toDF("id")
            .withColumn("dummy_col", ($"id" / 55).cast("int"))
            .withColumn(expectedRowIdxCol, ($"id" % numRecordsPerFile).cast("int"))

          // Add row index to schema if required.
          val schemaWithRowIdx = if (rowIndexMetadataColumnSupported) {
            df.schema
          } else {
            df.schema.add(rowIndexColName, LongType, nullable = true)
          }

          df.write
            .format(conf.writeFormat)
            .option(ParquetOutputFormat.BLOCK_SIZE, conf.rowGroupSize)
            .option(ParquetOutputFormat.PAGE_SIZE, conf.pageSize)
            .option(ParquetOutputFormat.DICTIONARY_PAGE_SIZE, conf.pageSize)
            .save(path.getAbsolutePath)
          val dfRead = spark.read
            .format(conf.readFormat)
            .schema(schemaWithRowIdx)
            .load(path.getAbsolutePath)

          // Verify that the produced files are laid out as expected.
          if (conf.useSmallRowGroups) {
            if (conf.useSmallPages) {
              assertIntermediateRowGroups(path)
            } else {
              assertTinyRowGroups(path)
            }
          } else {
            assertOneRowGroup(path)
          }

          val dfToAssert = if (conf.useFilter) {
            // Add a filter such that we skip 60% of the records:
            // [0%, 20%], [40%, 60%], [80%, 100%]
            dfRead.filter((
              $"id" >= (skipCentileFirst * conf.numRows).toInt &&
                $"id" < (skipCentileMidLeft * conf.numRows).toInt) || (
              $"id" >= (skipCentileMidRight * conf.numRows).toInt &&
                $"id" < (skipCentileLast * conf.numRows).toInt))
          } else {
            dfRead
          }

          var numPartitions: Long = 0
          var numOutputRows: Long = 0
          dfToAssert.collect()
          dfToAssert.queryExecution.executedPlan.foreach {
            case b: BatchScanExec =>
              numPartitions += b.inputRDD.partitions.length
              numOutputRows += b.metrics("numOutputRows").value
            case f: FileSourceScanExec =>
              numPartitions += f.inputRDD.partitions.length
              numOutputRows += f.metrics("numOutputRows").value
            case _ =>
          }
          assert(numPartitions > 0)
          assert(numOutputRows > 0)

          if (conf.useSmallSplits) {
            assert(numPartitions >= 2 * conf.numFiles)
          }

          // Assert that every rowIdx value matches the value in `expectedRowIdx`.
          assert(dfToAssert.filter(s"$rowIndexColName != $expectedRowIdxCol")
            .count() == 0)

          if (conf.useFilter) {
            if (conf.useSmallRowGroups) {
              assert(numOutputRows < conf.numRows)
            }

            val minMaxRowIndexes = dfToAssert.select(
              max(col(rowIndexColName)),
              min(col(rowIndexColName))).collect()
            val (expectedMaxRowIdx, expectedMinRowIdx) = if (conf.numFiles == 1) {
              // When there is a single file, we still have row group skipping,
              // but that should not affect the produced rowIdx.
              (conf.numRows * skipCentileLast - 1, conf.numRows * skipCentileFirst)
            } else {
              // For simplicity, the chosen filter skips the whole files.
              // Thus all unskipped files will have the same max and min rowIdx values.
              (numRecordsPerFile - 1, 0)
            }
            assert(minMaxRowIndexes(0).get(0) == expectedMaxRowIdx)
            assert(minMaxRowIndexes(0).get(1) == expectedMinRowIdx)
            if (!conf.useMultipleFiles) {
              val skippedValues = List.range(0, (skipCentileFirst * conf.numRows).toInt) ++
                List.range((skipCentileMidLeft * conf.numRows).toInt,
                  (skipCentileMidRight * conf.numRows).toInt) ++
                List.range((skipCentileLast * conf.numRows).toInt, conf.numRows)
              // rowIdx column should not have any of the `skippedValues`.
              assert(dfToAssert
                .filter(col(rowIndexColName).isin(skippedValues: _*)).count() == 0)
            }
          } else {
            assert(numOutputRows == conf.numRows)
            // When there is no filter, the rowIdx values should be in range
            // [0-`numRecordsPerFile`].
            val expectedRowIdxValues = List.range(0, numRecordsPerFile)
            assert(dfToAssert.filter(col(rowIndexColName).isin(expectedRowIdxValues: _*))
              .count() == conf.numRows)
          }
        }
      }
    }
  }

  for (useDataSourceV2 <- Seq(true, false)) {
    val conf = RowIndexTestConf(useDataSourceV2 = useDataSourceV2)

    test(s"invalid row index column type - ${conf.desc}") {
      withSQLConf(conf.sqlConfs: _*) {
        withTempPath{ path =>
          val df = spark.range(0, 10, 1, 1).toDF("id")
          val schemaWithRowIdx = df.schema
            .add(ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME, StringType)

          df.write
            .format(conf.writeFormat)
            .save(path.getAbsolutePath)

          val dfRead = spark.read
            .format(conf.readFormat)
            .schema(schemaWithRowIdx)
            .load(path.getAbsolutePath)

          val exception = intercept[SparkException](dfRead.collect())
          assert(exception.getCondition.startsWith("FAILED_READ_FILE"))
          assert(exception.getCause.getMessage.contains(
            ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME))
        }
      }
    }
  }
}
