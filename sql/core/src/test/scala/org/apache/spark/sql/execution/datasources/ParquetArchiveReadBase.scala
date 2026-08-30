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

package org.apache.spark.sql.execution.datasources

import java.io.File

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Binds [[ArchiveReadSuiteBase]]'s hooks to Parquet (entries unpacked to a local file for footer
 * random access). Parquet is self-describing, so the base's schema-inference tests run too.
 */
trait ParquetArchiveReadBase extends ArchiveReadSuiteBase {

  override protected def format: String = "parquet"

  override protected def fileExtension: String = "parquet"

  override protected def readOptions: Map[String, String] = Map.empty

  override protected def readSchema: String = "id INT, name STRING"

  // Parquet has authoritative per-file schemas and only unions under `mergeSchema`, so it opts out
  // of the by-name default-inference union (covered instead by the mergeSchema test below).
  override protected def supportsSchemaMerge: Boolean = false

  // Parquet samples one part-file for non-merge inference (SPARK-11500).
  override protected def inferenceSamplesOneFile: Boolean = true

  // Parquet unpacks each entry to a local temp file for footer random access.
  override protected def localizesEntries: Boolean = true

  override protected def archiveTempDirPrefix: String = "parquet-archive"

  override protected def vectorizedReaderConfKey: Option[String] =
    Some(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key)

  test("inference skips a missing archive among good ones (ignoreMissingFiles)") {
    // Exercised on ParquetFileFormat.inferSchema(files) directly: inference now runs on the
    // executor, so a hand-built missing status reaches the archive open and throws
    // FileNotFoundException at task time. The public read path re-lists and drops the missing path
    // before inference, so the missing-at-open window is only reachable through this entry point. A
    // missing archive is governed by ignoreMissingFiles, not ignoreCorruptFiles.
    withArchiveFile() { good =>
      writeArchive(good, Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice")))))
      val missing = new File(good.getParentFile, s"missing.${archiveExtensions.head}")
      val archives = Seq(
        new FileStatus(good.length(), false, 0, 0, good.lastModified(), new Path(good.toURI)),
        new FileStatus(1, false, 0, 0, 0, new Path(missing.toURI)))
      def infer(ignoreMissing: Boolean, ignoreCorrupt: Boolean = false): Option[StructType] =
        new ParquetFileFormat().inferSchema(spark, readOptions ++ Map(
          "ignoreMissingFiles" -> ignoreMissing.toString,
          "ignoreCorruptFiles" -> ignoreCorrupt.toString,
          "mergeSchema" -> "true"), archives)
      assert(infer(ignoreMissing = true).exists(_.fieldNames.contains("id")),
        "expected the surviving archive's schema")
      intercept[Exception](infer(ignoreMissing = false))
      intercept[Exception](infer(ignoreMissing = false, ignoreCorrupt = true))
    }
  }

}

class ParquetTarArchiveReadSuite
  extends ArchiveReadSuiteBase
  with ParquetArchiveReadBase
  with TarArchiveReadBase

class ParquetZipArchiveReadSuite
  extends ArchiveReadSuiteBase
  with ParquetArchiveReadBase
  with ZipArchiveReadBase

class ParquetSevenZArchiveReadSuite
  extends ArchiveReadSuiteBase
  with ParquetArchiveReadBase
  with SevenZArchiveReadBase
