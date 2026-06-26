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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

/**
 * Format- and archive-agnostic end-to-end tests for reading archives of data files through the
 * streaming [[ArchiveReader]] path. Entries are streamed (never unpacked to disk), and the central
 * contract verified throughout is parity with reading the same files from a directory.
 *
 * A concrete suite binds the abstract hooks below by mixing in a file-format trait (e.g.
 * [[org.apache.spark.sql.execution.datasources.CSVArchiveReadBase]]) and an archive-format trait
 * (e.g. [[TarArchiveReadBase]]), so the same tests run for every (file format, archive format)
 * pair we support. New formats are added by writing the per-format trait once, not by duplicating
 * these tests:
 * {{{
 *   class CSVHeaderTarArchiveReadSuite
 *     extends ArchiveReadSuiteBase with CSVHeaderArchiveReadBase with TarArchiveReadBase
 * }}}
 */
trait ArchiveReadSuiteBase extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ARCHIVE_FORMAT_READER_ENABLED.key, "true")

  import testImplicits._

  // ----- file-format hooks (bound by e.g. CSVArchiveReadBase) ----------------

  /** The `DataFrameReader`/`DataFrameWriter` format name, e.g. "csv". */
  protected def format: String

  /** Extension of a single data file of [[format]] inside an archive, e.g. "csv". */
  protected def fileExtension: String

  /** Read options applied to every read (e.g. CSV `header`). */
  protected def readOptions: Map[String, String]

  /** Schema used to read the sample data produced by [[sampleDf]]. */
  protected def readSchema: String

  /**
   * Encodes `df` as the bytes of a single data file of [[format]], honoring `writeOptions` (plus
   * [[readOptions]], so e.g. CSV writes with the same `header` mode it reads). Coalesces to one
   * partition and returns that single part file's bytes.
   */
  protected def encodeFile(df: DataFrame, writeOptions: Map[String, String]): Array[Byte] = {
    val dir = Utils.createTempDir(namePrefix = "archive-test-encode")
    try {
      df.coalesce(1).write.format(format)
        .options(readOptions ++ writeOptions)
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

  /** Encodes `df` as a single data file using only the format's default write options. */
  protected final def encodeFile(df: DataFrame): Array[Byte] = encodeFile(df, Map.empty)

  // ----- archive-format hooks (bound by e.g. TarArchiveReadBase) -------------

  /** Archive extensions to exercise, e.g. Seq("tar", "tar.gz", "tgz"). The head is the default. */
  protected def archiveExtensions: Seq[String]

  /** Writes `entries` (name -> bytes) into the archive at `dest`; compression follows the ext. */
  protected def writeArchive(dest: File, entries: Seq[(String, Array[Byte])]): Unit

  /** Writes bytes that are not a readable archive at `dest` (of [[corruptArchiveExtension]]). */
  protected def writeCorruptArchive(dest: File): Unit

  /** An archive extension whose reader fails on corrupt bytes (used by the corrupt-file tests). */
  protected def corruptArchiveExtension: String

  // ----- helpers -------------------------------------------------------------

  /** Sample two-column data; the column names line up with [[readSchema]]. */
  protected def sampleDf(rows: (Int, String)*): DataFrame = rows.toDF("id", "name")

  /** Entry file name for the i-th data file in an archive. */
  protected def entryName(i: Int): String = s"part-$i.$fileExtension"

  /** Provides an archive-extensioned path inside a fresh temp dir to `f` and returns its result. */
  protected def withArchiveFile[T](
      extension: String = archiveExtensions.head)(f: File => T): T = {
    val dir = Utils.createTempDir(namePrefix = "archive-test")
    try f(new File(dir, s"archive.$extension")) finally Utils.deleteRecursively(dir)
  }

  /** Reads `path` with the format, [[readOptions]] (plus `extraOptions`), and `schema`. */
  protected def read(
      path: String,
      extraOptions: Map[String, String] = Map.empty,
      schema: String = readSchema): DataFrame =
    spark.read.format(format).options(readOptions ++ extraOptions).schema(schema).load(path)

  /**
   * Whether this format can infer a read schema from the files (content or an embedded header).
   * Gates the shared schema-inference tests; a format that cannot infer (e.g. ORC) sets it false.
   * A format that infers but does not widen types across entries excludes that one test via
   * [[excluded]] (see Avro). CSV-style inference triggers also override [[inferenceOptions]].
   */
  protected def supportsSchemaInference: Boolean = true

  /** Extra options that trigger/control inference for [[supportsSchemaInference]] formats. */
  protected def inferenceOptions: Map[String, String] = Map.empty

  /**
   * Whether this format can represent nested/complex types (struct/array/map). Gates the shared
   * complex-type round-trip test; JSON keeps it, CSV and text override it to false.
   */
  protected def supportsComplexTypes: Boolean = true

  /**
   * Whether inputs with different field sets are unioned by field name -- the self-describing,
   * field-name-keyed model of JSON (and Avro/Parquet/XML), as opposed to CSV's positional,
   * first-header-keyed model. Gates the shared differing-field read/inference tests; CSV and text
   * override it to false.
   */
  protected def supportsSchemaMerge: Boolean = true

  /** Sample data with a nested struct column, used by the complex-type test. */
  protected def complexSampleDf: DataFrame =
    Seq((1, "NYC", "10001"), (2, "SF", "94105")).toDF("id", "city", "zip")
      .select(col("id"), struct(col("city"), col("zip")).as("addr"))

  /** Read schema matching [[complexSampleDf]]. */
  protected def complexReadSchema: String = "id INT, addr STRUCT<city: STRING, zip: STRING>"

  /**
   * Schema [[format]] infers from `paths` under [[readOptions]] ++ [[inferenceOptions]] (plus
   * `extraOptions`). Loading several paths reads them as one fileset, exactly as a directory
   * read does.
   */
  protected def inferredSchema(
      paths: Seq[String],
      extraOptions: Map[String, String] = Map.empty): StructType =
    spark.read.options(readOptions ++ inferenceOptions ++ extraOptions)
      .format(format).load(paths: _*).schema

  /**
   * Writes `entries` both into an archive and as loose files in a directory, then asserts the
   * archive read produces exactly the same rows as the directory read.
   */
  protected def assertArchiveMatchesDir(
      entries: Seq[(String, Array[Byte])],
      extraOptions: Map[String, String] = Map.empty,
      schema: String = readSchema): Unit = {
    withArchiveFile() { archive =>
      writeArchive(archive, entries)
      val fromArchive = read(archive.getCanonicalPath, extraOptions, schema)
      withTempDir { dir =>
        entries.foreach { case (name, b) => Files.write(new File(dir, name).toPath, b) }
        checkAnswer(fromArchive, read(dir.getCanonicalPath, extraOptions, schema).collect().toSeq)
      }
    }
  }

  // ----- tests ---------------------------------------------------------------

  test("read an archive of multiple entries matches the union of the inputs") {
    archiveExtensions.foreach { ext =>
      withArchiveFile(ext) { archive =>
        val parts = Seq(
          sampleDf((1, "Alice"), (2, "Bob")),
          sampleDf((3, "Carol")),
          sampleDf((4, "Dan"), (5, "Eve")))
        writeArchive(
          archive, parts.zipWithIndex.map { case (p, i) => entryName(i) -> encodeFile(p) })
        checkAnswer(read(archive.getCanonicalPath), parts.reduce(_ union _))
      }
    }
  }

  test("archive entries parse like a directory of the same files") {
    val parts = Seq(sampleDf((1, "Alice"), (2, "Bob")), sampleDf((3, "Carol")))
    assertArchiveMatchesDir(parts.zipWithIndex.map { case (p, i) => entryName(i) -> encodeFile(p) })
  }

  test("column pruning selects a subset of columns") {
    withArchiveFile() { archive =>
      val data = sampleDf((1, "Alice"), (2, "Bob"))
      writeArchive(archive, Seq(entryName(0) -> encodeFile(data)))
      checkAnswer(read(archive.getCanonicalPath).select("name"), Seq(Row("Alice"), Row("Bob")))
    }
  }

  test("multiple entries and multiple loose files under a partitioned dir, plus an empty archive") {
    withTempDir { rootDir =>
      val partitionDir = new File(rootDir, "dt=2024-01-01")
      assert(partitionDir.mkdirs())

      val inArchive = Seq(sampleDf((1, "in-archive-a")), sampleDf((2, "in-archive-b")))
      val loose = Seq(sampleDf((3, "loose-a")), sampleDf((4, "loose-b")))
      val ext = archiveExtensions.head

      writeArchive(
        new File(partitionDir, s"data.$ext"),
        inArchive.zipWithIndex.map { case (p, i) => entryName(i) -> encodeFile(p) })
      // An empty archive in the same directory must contribute no rows.
      writeArchive(new File(partitionDir, s"empty.$ext"), Seq.empty)
      loose.zipWithIndex.foreach { case (p, i) =>
        Files.write(new File(partitionDir, s"loose-$i.$fileExtension").toPath, encodeFile(p))
      }

      val expected = (inArchive ++ loose).reduce(_ union _)
      checkAnswer(read(rootDir.getCanonicalPath).select("id", "name"), expected)
    }
  }

  test("a directory of only empty archives yields no rows") {
    withTempDir { dir =>
      archiveExtensions.foreach { ext =>
        writeArchive(new File(dir, s"empty-${ext.replace('.', '_')}.$ext"), Seq.empty)
      }
      checkAnswer(read(dir.getCanonicalPath), Seq.empty[Row])
    }
  }

  test("an empty archive yields no rows") {
    withArchiveFile() { archive =>
      writeArchive(archive, Seq.empty)
      checkAnswer(read(archive.getCanonicalPath), Seq.empty[Row])
    }
  }

  test("an archive always yields a single partition regardless of size") {
    withArchiveFile() { archive =>
      val big = sampleDf((1 to 1000).map(i => (i, s"value-$i")): _*)
      writeArchive(archive, (0 until 4).map(i => entryName(i) -> encodeFile(big)))
      withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1024") {
        val readDf = read(archive.getCanonicalPath)
        assert(readDf.rdd.getNumPartitions == 1,
          s"archive should be a single partition; got ${readDf.rdd.getNumPartitions}")
        assert(readDf.count() == 4L * big.count())
      }
    }
  }

  Seq(true, false).foreach { ignoreCorrupt =>
    test(s"ignoreCorruptFiles=$ignoreCorrupt controls whether a corrupt archive is skipped") {
      withArchiveFile(corruptArchiveExtension) { archive =>
        writeCorruptArchive(archive)
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorrupt.toString) {
          if (ignoreCorrupt) {
            checkAnswer(read(archive.getCanonicalPath), Seq.empty[Row])
          } else {
            intercept[SparkException](read(archive.getCanonicalPath).collect())
          }
        }
      }
    }
  }

  test("a corrupt archive among good ones is skipped whole, not per entry (ignoreCorruptFiles)") {
    withTempDir { dir =>
      val good = sampleDf((1, "Alice"), (2, "Bob"))
      writeArchive(new File(dir, s"good.${archiveExtensions.head}"),
        Seq(entryName(0) -> encodeFile(good)))
      writeCorruptArchive(new File(dir, s"bad.$corruptArchiveExtension"))
      // A tar is one non-splittable unit, so corrupt handling is archive-granular: the corrupt
      // archive is skipped in its entirety while the good archive's rows are still returned.
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
        checkAnswer(read(dir.getCanonicalPath), good)
      }
    }
  }

  // ----- shared schema-inference tests (run when `supportsSchemaInference`) --

  if (supportsSchemaInference) {
    test("archive infers the same schema as a directory of the same files") {
      val entries = Seq(sampleDf((1, "Alice"), (2, "Bob")), sampleDf((3, "Carol")))
        .zipWithIndex.map { case (p, i) => entryName(i) -> encodeFile(p) }
      withArchiveFile() { archive =>
        writeArchive(archive, entries)
        val archiveSchema = inferredSchema(Seq(archive.getCanonicalPath))
        withTempDir { dir =>
          entries.foreach { case (n, b) => Files.write(new File(dir, n).toPath, b) }
          assert(archiveSchema == inferredSchema(Seq(dir.getCanonicalPath)),
            s"inference parity broken; archive=$archiveSchema")
        }
      }
    }

    test("all archive formats infer the same schema") {
      val entries = Seq(sampleDf((1, "Alice"), (2, "Bob")), sampleDf((3, "Carol")))
        .zipWithIndex.map { case (p, i) => entryName(i) -> encodeFile(p) }
      val schemas = archiveExtensions.map { ext =>
        withArchiveFile(ext) { archive =>
          writeArchive(archive, entries)
          inferredSchema(Seq(archive.getCanonicalPath))
        }
      }
      assert(schemas.distinct.size == 1,
        s"archive formats inferred different schemas: ${archiveExtensions.zip(schemas)}")
    }

    test("inference skips a corrupt archive among good ones (ignoreCorruptFiles)") {
      withTempDir { dir =>
        val good = sampleDf((1, "Alice"), (2, "Bob"))
        writeArchive(new File(dir, s"good.${archiveExtensions.head}"),
          Seq(entryName(0) -> encodeFile(good)))
        writeCorruptArchive(new File(dir, s"bad.$corruptArchiveExtension"))
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
          val schema = inferredSchema(Seq(dir.getCanonicalPath))
          withTempDir { onlyGood =>
            Files.write(new File(onlyGood, entryName(0)).toPath, encodeFile(good))
            assert(schema == inferredSchema(Seq(onlyGood.getCanonicalPath)),
              s"corrupt archive not skipped during inference; got $schema")
          }
        }
      }
    }

    test("inference merges archive entries with loose files in the same directory") {
      // An archive entry and a loose file with the same schema infer one combined schema, matching
      // a directory read of the same files.
      val inArchive = sampleDf((1, "Alice"), (2, "Bob"))
      val loose = sampleDf((3, "Carol"))
      withTempDir { dir =>
        writeArchive(new File(dir, s"data.${archiveExtensions.head}"),
          Seq(entryName(0) -> encodeFile(inArchive)))
        Files.write(new File(dir, s"loose.$fileExtension").toPath, encodeFile(loose))
        val schema = inferredSchema(Seq(dir.getCanonicalPath))
        withTempDir { looseDir =>
          Files.write(new File(looseDir, entryName(0)).toPath, encodeFile(inArchive))
          Files.write(new File(looseDir, s"loose.$fileExtension").toPath, encodeFile(loose))
          assert(schema == inferredSchema(Seq(looseDir.getCanonicalPath)),
            s"mixed archive+loose inference diverged from a directory read; got $schema")
        }
      }
    }
  }

  // ----- shared schema-merge tests (run when `supportsSchemaMerge`) ----------

  if (supportsSchemaMerge) {
    test("archive inference widens a column's type across entries like a directory") {
      // The column is integral in the first entry and string in the second; inference over all
      // entries widens the merged type to string, exactly as a directory read would. This is a
      // schema-merge behavior (cross-entry type reconciliation), so it is gated by
      // `supportsSchemaMerge` rather than `supportsSchemaInference`: a format can infer a single
      // file's schema without reconciling differing types across files (e.g. Parquet).
      withArchiveFile() { archive =>
        writeArchive(archive, Seq(
          entryName(0) -> encodeFile(Seq(1, 2).toDF("c")),
          entryName(1) -> encodeFile(Seq("x").toDF("c"))))
        val schema = inferredSchema(Seq(archive.getCanonicalPath))
        assert(schema.length == 1 && schema.head.dataType == StringType,
          s"expected the column widened to string across entries, got $schema")
      }
    }

    test("archive entries with differing fields read like a directory") {
      // One entry carries an extra field the other lacks; read under a schema covering both, the
      // missing field reads back null -- exactly as a directory read of the same files does.
      val withName = sampleDf((1, "Alice"), (2, "Bob"))
      val idOnly = Seq(3).toDF("id")
      assertArchiveMatchesDir(
        Seq(entryName(0) -> encodeFile(withName), entryName(1) -> encodeFile(idOnly)))
    }

    test("inference unions differing fields across archive entries and loose files") {
      // The archive entry has fields (id, name); the loose file has (id, extra). A field-name-keyed
      // format unions them by name -- exactly as a directory read of the same files does.
      val inArchive = sampleDf((1, "Alice"), (2, "Bob"))
      val loose = Seq((3, 30)).toDF("id", "extra")
      withTempDir { dir =>
        writeArchive(new File(dir, s"data.${archiveExtensions.head}"),
          Seq(entryName(0) -> encodeFile(inArchive)))
        Files.write(new File(dir, s"loose.$fileExtension").toPath, encodeFile(loose))
        val schema = inferredSchema(Seq(dir.getCanonicalPath))
        assert(schema.fieldNames.toSet == Set("id", "name", "extra"),
          s"expected the union of the entry and loose fields, got $schema")
        withTempDir { looseDir =>
          Files.write(new File(looseDir, entryName(0)).toPath, encodeFile(inArchive))
          Files.write(new File(looseDir, s"loose.$fileExtension").toPath, encodeFile(loose))
          assert(schema == inferredSchema(Seq(looseDir.getCanonicalPath)),
            s"differing-field inference diverged from a directory read; got $schema")
        }
      }
    }
  }

  // ----- shared complex-type test (run when `supportsComplexTypes`) ----------

  if (supportsComplexTypes) {
    test("archive round-trips nested struct types like a directory") {
      assertArchiveMatchesDir(
        Seq(entryName(0) -> encodeFile(complexSampleDf)),
        schema = complexReadSchema)
    }
  }
}
