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

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.Utils

/**
 * Binds [[ArchiveReadSuiteBase]]'s file-format hooks to CSV. The header-mode-specific tests live in
 * the [[CSVHeaderArchiveReadBase]] and [[CSVHeaderlessArchiveReadBase]] sub-traits, so the shared
 * archive tests from [[ArchiveReadSuiteBase]] run for both modes.
 */
trait CSVArchiveReadBase extends ArchiveReadSuiteBase {

  /** Whether the archived CSV files are written and read with a header row. */
  protected def header: Boolean

  override protected def format: String = "csv"

  override protected def fileExtension: String = "csv"

  override protected def readOptions: Map[String, String] = Map("header" -> header.toString)

  override protected def readSchema: String = "id INT, name STRING"

  override protected def encodeFile(
      df: DataFrame,
      writeOptions: Map[String, String]): Array[Byte] = {
    val dir = Utils.createTempDir(namePrefix = "archive-test-encode")
    try {
      df.coalesce(1).write.format("csv")
        .options(Map("header" -> header.toString) ++ writeOptions)
        .mode("overwrite").save(dir.getCanonicalPath)
      val parts = dir.listFiles().filter { f =>
        f.isFile && !f.getName.startsWith("_") && !f.getName.startsWith(".") &&
          !f.getName.endsWith(".crc")
      }
      assert(parts.length == 1,
        s"expected exactly one data file, got: ${parts.map(_.getName).toList}")
      Files.readAllBytes(parts.head.toPath)
    } finally Utils.deleteRecursively(dir)
  }

  /** Raw CSV bytes, for tests that need precise control over the row layout. */
  protected def csvBytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)

  test("CSV: reading an archive without a schema fails (inference not yet supported)") {
    // Schema inference for archives is a follow-up; until then an explicit schema is required, and
    // an inference attempt raises Spark's standard UNABLE_TO_INFER_SCHEMA error.
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob")))))
      val e = intercept[AnalysisException] {
        spark.read.format(format).options(readOptions).load(archive.getCanonicalPath)
      }
      assert(e.getCondition == "UNABLE_TO_INFER_SCHEMA",
        s"expected UNABLE_TO_INFER_SCHEMA, got ${e.getCondition}: ${e.getMessage}")
    }
  }
}
