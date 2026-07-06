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

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Binds [[ArchiveReadSuiteBase]]'s file-format hooks to Parquet. Unlike the streaming formats,
 * Parquet entries are unpacked to a local file before reading (Parquet needs random access to its
 * footer), but the read/inference parity contract the base verifies is identical. Parquet is
 * self-describing, so [[inferenceOptions]] stays empty and the base's schema-inference tests run as
 * well. There is no header concept, so (unlike CSV) there is a single base rather than header and
 * headerless variants.
 */
trait ParquetArchiveReadBase extends ArchiveReadSuiteBase {

  import testImplicits._

  override protected def format: String = "parquet"

  override protected def fileExtension: String = "parquet"

  override protected def readOptions: Map[String, String] = Map.empty

  override protected def readSchema: String = "id INT, name STRING"

  // Parquet has an authoritative per-file schema and only unions differing fields across files when
  // `mergeSchema` is set, so it does not union by name during default inference the way the
  // schema-on-read formats do. The differing-fields *read* (missing column -> null) is still
  // supported and is covered by a Parquet-specific test below.
  override protected def supportsSchemaMerge: Boolean = false

  override protected def encodeFile(
      df: DataFrame,
      writeOptions: Map[String, String]): Array[Byte] = {
    val dir = Utils.createTempDir(namePrefix = "archive-test-encode")
    try {
      df.coalesce(1).write.format("parquet")
        .options(writeOptions).mode("overwrite").save(dir.getCanonicalPath)
      val parts = dir.listFiles().filter { f =>
        f.isFile && !f.getName.startsWith("_") && !f.getName.startsWith(".") &&
          !f.getName.endsWith(".crc")
      }
      assert(parts.length == 1,
        s"expected exactly one data file, got: ${parts.map(_.getName).toList}")
      Files.readAllBytes(parts.head.toPath)
    } finally Utils.deleteRecursively(dir)
  }

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
      assert(paths.forall(_.contains(archive.getName)),
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

  Seq(true, false).foreach { ignoreCorrupt =>
    test(s"ignoreCorruptFiles=$ignoreCorrupt: inference skips a corrupt entry's whole archive") {
      // A corrupt entry condemns its whole archive during inference (no partial ingestion), so the
      // `extra` column carried by the bad archive's valid sibling entry must not surface. A good
      // archive is unaffected. mergeSchema=true folds every entry's footer.
      val merge = Map("mergeSchema" -> "true")
      val sibling = Seq((3, "Carol", "x")).toDF("id", "name", "extra")
      withTempDir { dir =>
        writeArchive(new File(dir, s"good.${archiveExtensions.head}"),
          Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob")))))
        writeArchive(new File(dir, s"bad.${archiveExtensions.head}"), Seq(
          entryName(0) -> encodeFile(sibling),
          entryName(1) -> "This is not a valid Parquet file".getBytes("UTF-8")))
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorrupt.toString) {
          if (ignoreCorrupt) {
            assert(inferredSchema(Seq(dir.getCanonicalPath), merge).fieldNames.toSet ==
              Set("id", "name"),
              "the whole corrupt archive, including its valid sibling entry, should be skipped")
          } else {
            intercept[Exception](inferredSchema(Seq(dir.getCanonicalPath), merge))
          }
        }
      }
    }

    test(s"ignoreCorruptFiles=$ignoreCorrupt: read skips the rest of an archive at a bad entry") {
      // The streaming read matches the other formats: entries before the corrupt one are ingested,
      // then the rest of that archive is skipped (not whole-archive atomic like inference). A
      // separate good archive is read in full.
      val alive = sampleDf((1, "Alice"), (2, "Bob"))
      val beforeCorrupt = sampleDf((3, "Carol"))
      withTempDir { dir =>
        writeArchive(new File(dir, s"good.${archiveExtensions.head}"),
          Seq(entryName(0) -> encodeFile(alive)))
        writeArchive(new File(dir, s"bad.${archiveExtensions.head}"), Seq(
          entryName(0) -> encodeFile(beforeCorrupt),
          entryName(1) -> "This is not a valid Parquet file".getBytes("UTF-8")))
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorrupt.toString) {
          if (ignoreCorrupt) {
            checkAnswer(read(dir.getCanonicalPath), alive.union(beforeCorrupt))
          } else {
            intercept[SparkException](read(dir.getCanonicalPath).collect())
          }
        }
      }
    }
  }
}
