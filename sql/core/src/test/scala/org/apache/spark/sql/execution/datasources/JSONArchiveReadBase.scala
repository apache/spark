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

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{NullType, StringType}
import org.apache.spark.util.Utils

/**
 * Binds [[ArchiveReadSuiteBase]]'s file-format hooks to JSON. JSON opts into the shared
 * schema-inference and complex-type tests (see `supportsSchemaInference`/`supportsComplexTypes`),
 * and adds the JSON-specific tests with no format-agnostic analogue: NullType canonicalization,
 * field-union/null-in-loose merging, and multi-line documents. Reusable across archive formats: a
 * `JSON<Container>ArchiveReadSuite` mixes this in alongside the archive-format trait.
 */
trait JSONArchiveReadBase extends ArchiveReadSuiteBase {

  override protected def format: String = "json"

  override protected def fileExtension: String = "json"

  // JSON records are self-describing, so no per-file read options are required.
  override protected def readOptions: Map[String, String] = Map.empty

  override protected def readSchema: String = "id INT, name STRING"

  // JSON infers its schema from record content, represents nested structs, and unions fields by
  // name, so it keeps all three capability defaults on (supportsSchemaInference,
  // supportsComplexTypes, supportsSchemaMerge). Inference needs no trigger option, so the inherited
  // `inferenceOptions` stays empty.

  override protected def encodeFile(
      df: DataFrame,
      writeOptions: Map[String, String]): Array[Byte] = {
    val dir = Utils.createTempDir(namePrefix = "archive-test-encode")
    try {
      df.coalesce(1).write.format("json")
        .options(writeOptions)
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

  /** Raw JSON bytes, for tests that need precise control over the record layout. */
  protected def jsonBytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)

  // ----- JSON-specific schema inference --------------------------------------
  // The format-agnostic parity/widening/corrupt-skip, schema-merge (differing-field union), and
  // complex-type tests run from ArchiveReadSuiteBase (gated by the `supports*` hooks above); the
  // tests below assert JSON-specific inference behavior -- NullType canonicalization, null-in-loose
  // widening, multi-line merging, and charset handling -- that has no format-agnostic analogue.
  // They use the shared `inferredSchema` helper from the base.

  test("JSON: a column null across all archive entries infers as string") {
    // Field `v` is null in every record across both entries, so each per-record type is NullType.
    // The single inference pass canonicalizes the surviving NullType to StringType at the end (a
    // valid schema, no NullType), matching a directory read of the same files.
    val entries = Seq(
      entryName(0) -> jsonBytes("{\"k\":1,\"v\":null}\n{\"k\":2,\"v\":null}\n"),
      entryName(1) -> jsonBytes("{\"k\":3,\"v\":null}\n"))
    withArchiveFile() { archive =>
      writeArchive(archive, entries)
      val archiveSchema = inferredSchema(Seq(archive.getCanonicalPath))
      assert(archiveSchema.forall(_.dataType != NullType),
        s"expected no NullType columns after canonicalization, got $archiveSchema")
      assert(archiveSchema.find(_.name == "v").exists(_.dataType == StringType),
        s"expected the all-null column to canonicalize to string, got $archiveSchema")
      withTempDir { dir =>
        entries.foreach { case (n, b) => Files.write(new File(dir, n).toPath, b) }
        val dirSchema = inferredSchema(Seq(dir.getCanonicalPath))
        assert(archiveSchema == dirSchema,
          s"all-null column inference diverged from a directory; " +
            s"archive=$archiveSchema dir=$dirSchema")
      }
    }
  }

  test("JSON: multiline inference merges archive entries with loose files") {
    // multiLine + mixed inputs: inference reads every archive entry and loose file as one whole
    // document and infers a single schema, matching a directory read of the same files.
    val opts = Map("multiLine" -> "true")
    withTempDir { dir =>
      writeArchive(new File(dir, s"data.${archiveExtensions.head}"),
        Seq(entryName(0) -> jsonBytes("{\n  \"id\": 1,\n  \"name\": \"Alice\"\n}")))
      Files.write(new File(dir, s"loose.$fileExtension").toPath,
        jsonBytes("{\n  \"id\": 2,\n  \"age\": 30\n}"))
      val schema = inferredSchema(Seq(dir.getCanonicalPath), opts)
      withTempDir { looseDir =>
        Files.write(new File(looseDir, entryName(0)).toPath,
          jsonBytes("{\n  \"id\": 1,\n  \"name\": \"Alice\"\n}"))
        Files.write(new File(looseDir, s"loose.$fileExtension").toPath,
          jsonBytes("{\n  \"id\": 2,\n  \"age\": 30\n}"))
        val dirSchema = inferredSchema(Seq(looseDir.getCanonicalPath), opts)
        assert(schema == dirSchema,
          s"multiline mixed inference diverged from a directory read; got $schema want $dirSchema")
      }
    }
  }

  test("JSON: a field typed in the archive but null in a loose file is not collapsed to string") {
    // One inference pass over all inputs keeps the loose file's null `v` as NullType until the end,
    // so it widens with the archive's int. Inferring the loose file alone and merging two finished
    // schemas would have canonicalized `v` to string first and yielded string here -- diverging
    // from a directory read of the same files.
    val archived = jsonBytes("{\"k\":1,\"v\":10}\n{\"k\":2,\"v\":20}\n")
    val loose = jsonBytes("{\"k\":3,\"v\":null}\n")
    withTempDir { dir =>
      writeArchive(new File(dir, s"data.${archiveExtensions.head}"), Seq(entryName(0) -> archived))
      Files.write(new File(dir, s"loose.$fileExtension").toPath, loose)
      val schema = inferredSchema(Seq(dir.getCanonicalPath))
      withTempDir { looseDir =>
        Files.write(new File(looseDir, entryName(0)).toPath, archived)
        Files.write(new File(looseDir, s"loose.$fileExtension").toPath, loose)
        assert(schema == inferredSchema(Seq(looseDir.getCanonicalPath)),
          s"null-in-loose field diverged from a directory read; got $schema")
      }
      assert(schema.find(_.name == "v").exists(_.dataType != StringType),
        s"field null in the loose file should widen with the archive int, not collapse: $schema")
    }
  }

  test("JSON: archive inference honors the encoding option like a directory") {
    // A non-UTF-8 (UTF-16) archive: inference must decode each entry with `encoding`, exactly as
    // the scan and a directory read do. Reading the bytes as UTF-8 would mis-parse them, diverging
    // from a directory read of the same files. multiLine makes the whole document one record, so
    // line-separator handling does not enter the test.
    val opts = Map("encoding" -> "UTF-16", "multiLine" -> "true")
    val bytes = "{\n  \"id\": 1,\n  \"name\": \"Alice\"\n}".getBytes(StandardCharsets.UTF_16)
    withTempDir { dir =>
      writeArchive(new File(dir, s"data.${archiveExtensions.head}"), Seq(entryName(0) -> bytes))
      val schema = inferredSchema(Seq(dir.getCanonicalPath), opts)
      withTempDir { looseDir =>
        Files.write(new File(looseDir, entryName(0)).toPath, bytes)
        val dirSchema = inferredSchema(Seq(looseDir.getCanonicalPath), opts)
        assert(schema == dirSchema,
          s"encoding inference diverged from a directory read; archive=$schema dir=$dirSchema")
      }
      assert(schema.fieldNames.toSet == Set("id", "name"),
        s"expected id/name decoded from UTF-16, got $schema")
    }
  }

  test("JSON: archive inference auto-detects a non-UTF-8 charset with no encoding option") {
    // With no `encoding` set, inference parses each record from its raw bytes, so Jackson
    // auto-detects the charset (here UTF-16, carrying a BOM) exactly as the scan and a directory
    // read do. Forcing the bytes through UTF-8 would mis-parse them into a corrupt-record-only
    // schema, diverging from a directory read of the same files.
    val opts = Map("multiLine" -> "true")
    val bytes = "{\n  \"id\": 1,\n  \"name\": \"Alice\"\n}".getBytes(StandardCharsets.UTF_16)
    withTempDir { dir =>
      writeArchive(new File(dir, s"data.${archiveExtensions.head}"), Seq(entryName(0) -> bytes))
      val schema = inferredSchema(Seq(dir.getCanonicalPath), opts)
      withTempDir { looseDir =>
        Files.write(new File(looseDir, entryName(0)).toPath, bytes)
        val dirSchema = inferredSchema(Seq(looseDir.getCanonicalPath), opts)
        assert(schema == dirSchema,
          s"auto-detected inference diverged from a directory read; archive=$schema dir=$dirSchema")
      }
      assert(schema.fieldNames.toSet == Set("id", "name"),
        s"expected id/name auto-detected from UTF-16, got $schema")
    }
  }

  // ----- JSON-specific read tests --------------------------------------------

  test("JSON: multi-line documents match a directory read") {
    assertArchiveMatchesDir(
      Seq(
        "a.json" -> jsonBytes("{\n  \"id\": 1,\n  \"name\": \"Alice\"\n}"),
        "b.json" -> jsonBytes("{\n  \"id\": 2,\n  \"name\": \"Bob\"\n}")),
      extraOptions = Map("multiLine" -> "true"))
  }

  test("JSON: a malformed record in an archive entry matches a directory read (both modes)") {
    // Permissive mode (the default): a malformed record parses to nulls with its raw text echoed
    // into `_corrupt_record`. The archive path wires its own FailureSafeParser in `readStream` --
    // and in multiLine it echoes the entry's pre-buffered bytes rather than re-reading the file --
    // so assert the corrupt-record column matches a directory read of the same files in both the
    // line-delimited and whole-document modes.
    val corruptSchema = s"$readSchema, _corrupt_record STRING"
    // Line-delimited: a good record, then a malformed one on the next line.
    assertArchiveMatchesDir(
      Seq("a.json" -> jsonBytes("{\"id\":1,\"name\":\"Alice\"}\n{ not valid json\n")),
      schema = corruptSchema)
    // multiLine: the whole entry is one malformed document.
    assertArchiveMatchesDir(
      Seq("a.json" -> jsonBytes("{ not valid json")),
      extraOptions = Map("multiLine" -> "true"),
      schema = corruptSchema)
  }

  test("JSON: the DSv2 path refuses to infer a schema for an archive (UNABLE_TO_INFER_SCHEMA)") {
    // Archive scanning is wired into the v1 file source only, so the DSv2 reader cannot read
    // archives. On the v2 path inference must keep returning None for an archive input -- raising
    // UNABLE_TO_INFER_SCHEMA -- rather than inferring a schema the v2 scan would then mis-read as
    // raw archive bytes. Forcing json off the v1 source list routes the read through JsonTable.
    withArchiveFile() { archive =>
      writeArchive(archive, Seq(entryName(0) -> encodeFile(sampleDf((1, "Alice"), (2, "Bob")))))
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val e = intercept[AnalysisException] {
          spark.read.options(readOptions).format(format).load(archive.getCanonicalPath)
        }
        assert(e.getCondition == "UNABLE_TO_INFER_SCHEMA",
          s"expected UNABLE_TO_INFER_SCHEMA on the DSv2 path, got " +
            s"${e.getCondition}: ${e.getMessage}")
      }
    }
  }
}
