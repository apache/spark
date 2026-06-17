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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils

/**
 * Binds [[ArchiveReadSuiteBase]]'s file-format hooks to XML. XML opts into the shared
 * schema-inference and complex-type tests (see `supportsSchemaInference`/`supportsComplexTypes`),
 * and adds the XML-specific tests with no format-agnostic analogue: multi-line records,
 * element-union across entries, and attributes. Records are delimited by a `rowTag` (here `row`),
 * so a single `rowTag` is used for both writing and reading. Reusable across archive formats: a
 * `XML<Container>ArchiveReadSuite` mixes this in alongside the archive-format trait.
 */
trait XMLArchiveReadBase extends ArchiveReadSuiteBase {

  private val rowTag = "row"

  override protected def format: String = "xml"

  override protected def fileExtension: String = "xml"

  override protected def readOptions: Map[String, String] = Map("rowTag" -> rowTag)

  override protected def readSchema: String = "id INT, name STRING"

  // XML infers from record content, unions fields across inputs by name, and represents nested
  // elements as structs, so it keeps all three `supports*` defaults (inference, schema-merge,
  // complex types) and runs the full shared test set. Inference needs no trigger option, so
  // `inferenceOptions` keeps its empty default.

  override protected def encodeFile(
      df: DataFrame,
      writeOptions: Map[String, String]): Array[Byte] = {
    val dir = Utils.createTempDir(namePrefix = "archive-test-encode")
    try {
      df.coalesce(1).write.format("xml")
        .options(Map("rowTag" -> rowTag) ++ writeOptions)
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

  /** Raw XML bytes, for tests that need precise control over the record layout. */
  protected def xmlBytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)

  // ----- XML-specific tests --------------------------------------------------

  test("XML: records spanning multiple lines match a directory read") {
    assertArchiveMatchesDir(
      Seq(
        "a.xml" -> xmlBytes(
          "<rows>\n  <row>\n    <id>1</id>\n    <name>Alice</name>\n  </row>\n</rows>\n"),
        "b.xml" -> xmlBytes(
          "<rows>\n  <row>\n    <id>2</id>\n    <name>Bob</name>\n  </row>\n</rows>\n")))
  }

  test("XML: attributes match a directory read") {
    assertArchiveMatchesDir(
      Seq(
        "a.xml" -> xmlBytes("<rows><row id=\"1\"><name>Alice</name></row></rows>"),
        "b.xml" -> xmlBytes("<rows><row id=\"2\"><name>Bob</name></row></rows>")),
      schema = "_id INT, name STRING")
  }

  test("XML: inference widens a null archive field against a typed loose file like a directory") {
    // `c` is empty (NullType) in the archive entry and an integer in the loose file. A single
    // inference pass widens `c` to the integer type, exactly as a directory read does. Inferring
    // the archive and the loose file separately would canonicalize the archive's `c` to string
    // first, then merge to string -- diverging from the directory read.
    val inArchive = xmlBytes("<rows><row><id>1</id><c></c></row></rows>")
    val loose = xmlBytes("<rows><row><id>2</id><c>5</c></row></rows>")
    withTempDir { dir =>
      writeArchive(
        new File(dir, s"data.${archiveExtensions.head}"), Seq(entryName(0) -> inArchive))
      Files.write(new File(dir, s"loose.$fileExtension").toPath, loose)
      val schema = inferredSchema(Seq(dir.getCanonicalPath))
      assert(schema.find(_.name == "c").exists(_.dataType != StringType),
        s"expected `c` to widen to its real type, not collapse to string; got $schema")
      withTempDir { looseDir =>
        Files.write(new File(looseDir, entryName(0)).toPath, inArchive)
        Files.write(new File(looseDir, s"loose.$fileExtension").toPath, loose)
        assert(schema == inferredSchema(Seq(looseDir.getCanonicalPath)),
          s"archive+loose inference diverged from a directory read; got $schema")
      }
    }
  }

  test("XML: single-line mode reads and infers an archive like a directory") {
    // multiLine=false: each line is one record in both the scan and inference, matching a
    // non-archive single-line read. (The default multiLine=true is covered by the tests above.)
    val opts = Map("multiLine" -> "false")
    val entries = Seq(
      entryName(0) -> xmlBytes(
        "<row><id>1</id><name>Alice</name></row>\n<row><id>2</id><name>Bob</name></row>\n"),
      entryName(1) -> xmlBytes("<row><id>3</id><name>Carol</name></row>\n"))
    assertArchiveMatchesDir(entries, extraOptions = opts)
    withTempDir { dir =>
      writeArchive(new File(dir, s"data.${archiveExtensions.head}"), entries)
      val archiveSchema = inferredSchema(Seq(dir.getCanonicalPath), opts)
      withTempDir { looseDir =>
        entries.foreach { case (n, b) => Files.write(new File(looseDir, n).toPath, b) }
        assert(archiveSchema == inferredSchema(Seq(looseDir.getCanonicalPath), opts),
          s"single-line archive inference diverged from a directory read; got $archiveSchema")
      }
    }
  }
}
