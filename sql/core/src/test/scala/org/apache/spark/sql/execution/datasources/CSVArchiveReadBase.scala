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
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

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

  // CSV infers its schema from row content (supportsSchemaInference defaults true); inference is
  // triggered by the `inferSchema` option. CSV is positional/header-keyed and cannot represent
  // nested types, so it opts out of the schema-merge and complex-type tests.
  override protected def inferenceOptions: Map[String, String] = Map("inferSchema" -> "true")

  override protected def supportsComplexTypes: Boolean = false

  override protected def supportsSchemaMerge: Boolean = false

  /** Raw CSV bytes, for tests that need precise control over the row layout. */
  protected def csvBytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)

  /** CSV bytes for `rows`, prefixed with a `cols` header line when [[header]] is set. */
  private def csvEntry(cols: String, rows: String*): Array[Byte] =
    csvBytes((if (header) cols +: rows else rows).mkString("", "\n", "\n"))

  test("CSV: a column empty in the archive but typed in a loose file is not collapsed to string") {
    // One inference pass over all inputs keeps the empty column NullType until the end, so it
    // widens with the loose file's Int. Merging two already-finished schemas would have collapsed
    // the archive side to String first and yielded String here.
    withTempDir { dir =>
      writeArchive(new File(dir, s"data.${archiveExtensions.head}"),
        Seq(entryName(0) -> csvEntry("a,b", "1,", "2,")))
      Files.write(new File(dir, s"loose.$fileExtension").toPath, csvEntry("a,b", "3,4"))
      val schema = spark.read.options(readOptions).option("inferSchema", "true")
        .format(format).load(dir.getCanonicalPath).schema
      assert(schema.length == 2 && schema(1).dataType != StringType,
        s"empty-in-archive column should widen with the loose Int, not collapse to String: $schema")
    }
  }

  test("CSV: archive inference fixes the column count from the first entry's header") {
    // The first entry has two columns, the second three; one inference pass keys on the first
    // header, so the extra column in the later entry is dropped -- the same first-header-width
    // model a single-pass directory read uses.
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(
        entryName(0) -> csvEntry("a,b", "1,2"),
        entryName(1) -> csvEntry("a,b,c", "3,4,5")))
      val schema = spark.read.options(readOptions).option("inferSchema", "true")
        .format(format).load(archive.getCanonicalPath).schema
      assert(schema.length == 2 && schema.forall(_.dataType != StringType),
        s"expected 2 typed columns fixed by the first entry's header, got $schema")
    }
  }

  test("CSV: inference uses the same record model as the scan (quoted embedded newline)") {
    // In default (non-multiLine) mode the scan reads line by line, so a quoted field containing a
    // newline is split across rows; inference must tokenize the archived entry the same way, so it
    // infers the same schema as that entry read as a loose file (rather than parsing the entry as
    // one continuous stream and disagreeing with the read).
    val entry = csvEntry("a,b", "\"x\ny\",2")
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(entryName(0) -> entry))
      val archiveSchema = spark.read.options(readOptions).option("inferSchema", "true")
        .format(format).load(archive.getCanonicalPath).schema
      withTempDir { dir =>
        Files.write(new File(dir, entryName(0)).toPath, entry)
        val dirSchema = spark.read.options(readOptions).option("inferSchema", "true")
          .format(format).load(dir.getCanonicalPath).schema
        assert(archiveSchema == dirSchema,
          s"archive inference diverged from the line-based read; " +
            s"archive=$archiveSchema dir=$dirSchema")
      }
    }
  }

  test("CSV: archive inference under multiLine matches the scan (quoted embedded newline)") {
    // Under multiLine the scan reads each entry as one stream, so a quoted embedded newline is one
    // record; inference dispatches to the same stream model and agrees with that entry read as a
    // loose multiLine file. Pins the multiLine branch of the per-mode tokenization dispatch.
    val entry = csvEntry("a,b", "\"x\ny\",2")
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(entryName(0) -> entry))
      val archiveSchema = spark.read.options(readOptions).option("inferSchema", "true")
        .option("multiLine", "true").format(format).load(archive.getCanonicalPath).schema
      withTempDir { dir =>
        Files.write(new File(dir, entryName(0)).toPath, entry)
        val dirSchema = spark.read.options(readOptions).option("inferSchema", "true")
          .option("multiLine", "true").format(format).load(dir.getCanonicalPath).schema
        assert(archiveSchema == dirSchema,
          s"multiLine archive inference diverged from the multiLine read; " +
            s"archive=$archiveSchema dir=$dirSchema")
      }
    }
  }

  test("CSV: the DSv2 path refuses to infer a schema for an archive (UNABLE_TO_INFER_SCHEMA)") {
    // Archive scanning is wired into the V1 file source only, so the DSv2 reader cannot read
    // archives. On the V2 path inference must keep returning None for an archive input -- raising
    // UNABLE_TO_INFER_SCHEMA -- rather than inferring a schema and letting the V2 scan parse the
    // raw archive bytes as CSV. Forcing csv off the V1 source list routes the read through
    // CSVTable.
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob")))))
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val e = intercept[AnalysisException] {
          spark.read.options(readOptions).option("inferSchema", "true")
            .format(format).load(archive.getCanonicalPath)
        }
        assert(e.getCondition == "UNABLE_TO_INFER_SCHEMA",
          s"expected UNABLE_TO_INFER_SCHEMA on the DSv2 path, " +
            s"got ${e.getCondition}: ${e.getMessage}")
      }
    }
  }
}

/**
 * [[CSVArchiveReadBase]] reading CSV files that carry a header row, plus header-specific archive
 * tests (mismatched headers, and delimiter/multiline cases whose first row is a header).
 */
trait CSVHeaderArchiveReadBase extends CSVArchiveReadBase {

  import testImplicits._

  override protected def header: Boolean = true

  test("CSV: entries with mismatched headers behave like standalone files") {
    assertArchiveMatchesDir(
      Seq(
        entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob"))),
        // A different second-column header: the schema's "name" column is absent from this entry.
        entryName(1) -> encodeFile(Seq((3, "Carol")).toDF("id", "nickname"))))
  }

  test("CSV: custom delimiter matches a directory read") {
    assertArchiveMatchesDir(
      Seq("a.csv" -> csvBytes("id;name\n1;Alice\n2;Bob\n")),
      extraOptions = Map("delimiter" -> ";"))
  }

  test("CSV: multiline quoted fields with embedded newlines match a directory read") {
    assertArchiveMatchesDir(
      Seq(
        "a.csv" -> csvBytes("id,note\n1,\"line1\nline2\"\n2,\"plain\"\n"),
        "b.csv" -> csvBytes("id,note\n3,\"a\nb\nc\"\n")),
      extraOptions = Map("multiLine" -> "true"),
      schema = "id INT, note STRING")
  }
}

/**
 * [[CSVArchiveReadBase]] reading headerless CSV files (columns are positional), plus headerless
 * delimiter/multiline archive tests. The shared archive tests from [[ArchiveReadSuiteBase]] cover
 * the common headerless read paths.
 */
trait CSVHeaderlessArchiveReadBase extends CSVArchiveReadBase {

  override protected def header: Boolean = false

  test("CSV: headerless custom delimiter matches a directory read") {
    assertArchiveMatchesDir(
      Seq("a.csv" -> csvBytes("1;Alice\n2;Bob\n"), "b.csv" -> csvBytes("3;Carol\n")),
      extraOptions = Map("delimiter" -> ";"))
  }

  test("CSV: headerless multiline quoted fields with embedded newlines match a directory read") {
    assertArchiveMatchesDir(
      Seq(
        "a.csv" -> csvBytes("1,\"line1\nline2\"\n2,\"plain\"\n"),
        "b.csv" -> csvBytes("3,\"a\nb\nc\"\n")),
      extraOptions = Map("multiLine" -> "true"),
      schema = "id INT, note STRING")
  }
}

class CSVHeaderTarArchiveReadSuite
  extends ArchiveReadSuiteBase
  with CSVHeaderArchiveReadBase
  with TarArchiveReadBase

class CSVHeaderZipArchiveReadSuite
  extends ArchiveReadSuiteBase
  with CSVHeaderArchiveReadBase
  with ZipArchiveReadBase

class CSVHeaderSevenZArchiveReadSuite
  extends ArchiveReadSuiteBase
  with CSVHeaderArchiveReadBase
  with SevenZArchiveReadBase

class CSVHeaderlessTarArchiveReadSuite
  extends ArchiveReadSuiteBase
  with CSVHeaderlessArchiveReadBase
  with TarArchiveReadBase

class CSVHeaderlessZipArchiveReadSuite
  extends ArchiveReadSuiteBase
  with CSVHeaderlessArchiveReadBase
  with ZipArchiveReadBase

class CSVHeaderlessSevenZArchiveReadSuite
  extends ArchiveReadSuiteBase
  with CSVHeaderlessArchiveReadBase
  with SevenZArchiveReadBase
