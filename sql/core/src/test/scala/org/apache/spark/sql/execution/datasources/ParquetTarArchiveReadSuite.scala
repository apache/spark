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
import java.nio.file.Files

import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.internal.SQLConf

/**
 * Reads of Parquet files packed in tar archives (`.tar`/`.tar.gz`/`.tgz`): the shared archive
 * read/inference parity tests from [[ArchiveReadSuiteBase]] (bound to Parquet by
 * [[ParquetArchiveReadBase]] over tar containers via [[TarArchiveReadBase]]), plus Parquet-specific
 * tests for the unpack-to-disk read path.
 */
class ParquetTarArchiveReadSuite
  extends ArchiveReadSuiteBase
  with ParquetArchiveReadBase
  with TarArchiveReadBase {

  import testImplicits._

  for (vectorized <- Seq(true, false)) {
    test(s"archive reads return the same rows with vectorized reader = $vectorized") {
      // Parquet entries are read with the vectorized (columnar) reader or the row-based reader
      // depending on this flag; both must read archive entries identically to a directory read.
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
        assertArchiveMatchesDir(
          Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob")))))
      }
    }
  }

  test("input_file_name reports the archive path, not the unpacked temp file") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob")))))
      val paths = read(archive.getCanonicalPath)
        .select(input_file_name()).distinct().collect().map(_.getString(0))
      assert(paths.forall(_.contains("archive.tar")),
        s"expected the archive path, got: ${paths.toList}")
      assert(paths.forall(p => !p.contains("parquet-archive")),
        s"the unpacked temp path leaked into input_file_name: ${paths.toList}")
    }
  }

  test("an abandoned read (LIMIT) over an archive returns partial rows and cleans up") {
    withArchiveFile() { archive =>
      val parts = (0 until 4).map(i => entryName(i) -> encodeFile(sampleDf((i, s"v$i"))))
      writeArchive(archive, parts)
      // LIMIT stops iteration before later entries are reached; the task-completion listener
      // removes the temp dir. Assert the query runs and returns exactly the requested rows.
      assert(read(archive.getCanonicalPath).limit(2).collect().length == 2)
    }
  }

  test("archive entries with differing fields read like a directory") {
    // Parquet does not union schemas during inference (supportsSchemaMerge = false), but reading
    // entries with differing fields under a covering schema still fills the missing column with
    // null, exactly as a directory read of the same files does.
    val withName = sampleDf((1, "Alice"), (2, "Bob"))
    val idOnly = Seq(3).toDF("id")
    assertArchiveMatchesDir(
      Seq(entryName(0) -> encodeFile(withName), entryName(1) -> encodeFile(idOnly)))
  }

  test("archive inference unions differing fields across entries with mergeSchema=true") {
    // Parquet does not union schemas during default inference, but `mergeSchema=true` folds every
    // entry's schema; over an archive that folds each unpacked entry one at a time. The unioned
    // schema must match a directory read of the same files under the same option.
    val withName = sampleDf((1, "Alice"), (2, "Bob"))
    val idExtra = Seq((3, 30)).toDF("id", "extra")
    val entries = Seq(entryName(0) -> encodeFile(withName), entryName(1) -> encodeFile(idExtra))
    val merge = Map("mergeSchema" -> "true")
    withArchiveFile() { archive =>
      writeArchive(archive, entries)
      val archiveSchema = inferredSchema(Seq(archive.getCanonicalPath), merge)
      withTempDir { dir =>
        entries.foreach { case (n, b) => Files.write(new File(dir, n).toPath, b) }
        assert(archiveSchema.fieldNames.toSet == Set("id", "name", "extra"),
          s"expected the union of entry fields, got $archiveSchema")
        assert(archiveSchema == inferredSchema(Seq(dir.getCanonicalPath), merge),
          s"archive mergeSchema inference diverged from a directory read; got $archiveSchema")
      }
    }
  }
}
