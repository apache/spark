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

package org.apache.spark.sql.execution.datasources.json

import java.io.{File, FileOutputStream, StringReader}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.zip.GZIPOutputStream

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class EmbeddedArraySplitterSuite extends SparkFunSuite {
  private def split(input: String, arrayFieldName: String = "Records"): Seq[String] = {
    val splitter = new EmbeddedArraySplitter(new StringReader(input), arrayFieldName)
    Iterator.continually(splitter.nextRecord()).takeWhile(_.isDefined).map(_.get).toSeq
  }

  test("basic record splitting") {
    assert(split("""{"Records": [{"a": 1}, {"b": 2}]}""") === Seq("""{"a": 1}""", """{"b": 2}"""))
    assert(split("""{"Records":[{"a":1}]}""") === Seq("""{"a":1}"""))
    assert(split("""{"Records": []}""") === Seq.empty)
  }

  test("records with nesting, strings, and newlines") {
    val record1 =
      """{
        |  "a": {"nested": [1, 2, {"deep": "}]ignore me[{"}]},
        |  "b": "quote \" and backslash \\ and comma , and bracket ]"
        |}""".stripMargin
    val record2 = """{"Records": "a record field can also be called Records"}"""
    val input = s"""{"Records": [$record1,\n$record2\n]}"""
    assert(split(input) === Seq(record1, record2))
  }

  test("non-record content is ignored") {
    val input =
      """{
        |  "before": {"Records": [{"not": "a real record"}]},
        |  "alsoBefore": [1, [2, {"x": "]"}]],
        |  "Records": [{"a": 1}],
        |  "after": "ignored"
        |}""".stripMargin
    assert(split(input) === Seq("""{"a": 1}"""))
  }

  test("scalar records") {
    assert(split("""{"Records": [1, true, null, "s", 2.5]}""") ===
      Seq("1", "true", "null", "\"s\"", "2.5"))
  }

  test("no matching array field yields nothing") {
    assert(split("") === Seq.empty)
    assert(split("   ") === Seq.empty)
    assert(split("""{"a": 1}""") === Seq.empty)
    // The field value is not an array.
    assert(split("""{"Records": {"a": 1}}""") === Seq.empty)
    // The top-level value is not an object (e.g. ndjson input).
    assert(split("""[{"a": 1}]""") === Seq.empty)
    assert(split("{\"a\": 1}\n{\"b\": 2}") === Seq.empty)
  }

  test("custom array field name") {
    val input = """{"Records": [{"a": 1}], "events": [{"b": 2}, {"c": 3}]}"""
    assert(split(input, "events") === Seq("""{"b": 2}""", """{"c": 3}"""))
    assert(split(input) === Seq("""{"a": 1}"""))
    // The field name is matched case-sensitively and by raw text: a key written with escape
    // sequences in the document does not match.
    assert(split(input, "EVENTS") === Seq.empty)
    assert(split("{\"ev\\u0065nts\": [{\"b\": 2}]}", "events") === Seq.empty)
  }

  test("byte order mark is skipped") {
    assert(split("\uFEFF" + """{"Records": [{"a": 1}]}""") === Seq("""{"a": 1}"""))
  }

  test("truncated input returns the partial record") {
    assert(split("""{"Records": [{"a": 1}, {"b": """) === Seq("""{"a": 1}""", """{"b": """))
    assert(split("""{"Records": [{"a": "unclosed """) === Seq("""{"a": "unclosed """))
    // Truncated before any record.
    assert(split("""{"Records": """) === Seq.empty)
    assert(split("""{"Records"""") === Seq.empty)
  }

  test("input larger than the internal buffer") {
    val records = (0 until 10000).map { i =>
      s"""{"id": $i, "payload": "${"x" * (i % 97)}", "nested": {"level": [$i, ${i + 1}]}}"""
    }
    val input = s"""{"head": "ignored", "Records": [${records.mkString(", ")}], "tail": 1}"""
    assert(input.length > 64 * 1024)
    assert(split(input) === records)
  }
}

abstract class ExplodeEmbeddedArrayJsonSuite extends QueryTest with SharedSparkSession {
  private val documentContent =
    """{
      |  "unrelatedField": {"Records": [{"fake": 1}]},
      |  "Records": [
      |    {"eventName": "GetObject",
      |     "requestParameters": {"bucketName": "b1"}},
      |    {"eventName": "PutObject", "requestParameters": null},
      |    "scalar record",
      |    [1, 2]
      |  ],
      |  "trailing": "ignored"
      |}""".stripMargin

  private val expectedRecords = Seq(
    Row("""{"eventName":"GetObject","requestParameters":{"bucketName":"b1"}}"""),
    Row("""{"eventName":"PutObject","requestParameters":null}"""),
    Row("\"scalar record\""),
    Row("[1,2]"))

  private def writeFile(dir: File, name: String, content: String): File = {
    val file = new File(dir, name)
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
    file
  }

  protected def writeGzipFile(dir: File, name: String, content: String): File = {
    val file = new File(dir, name)
    val out = new GZIPOutputStream(new FileOutputStream(file))
    try {
      out.write(content.getBytes(StandardCharsets.UTF_8))
    } finally {
      out.close()
    }
    file
  }

  test("basic embedded array scan") {
    withTempDir { dir =>
      writeFile(dir, "file.json", documentContent)
      val df = spark.read.format("json").option("explodeEmbeddedArray", "Records")
        .load(dir.getAbsolutePath)
      // The inferred schema names the variant column after the embedded array field.
      assert(df.schema === StructType.fromDDL("Records variant"))
      checkAnswer(df.selectExpr("to_json(Records)"), expectedRecords)
      assert(df.count() === 4)
    }
  }

  test("array field name other than Records") {
    withTempDir { dir =>
      writeFile(dir, "file.json",
        """{"Records": [{"ignored": 1}], "events": [{"a": 1}, {"a": 2}]}""")
      val df = spark.read.format("json").option("explodeEmbeddedArray", "events")
        .load(dir.getAbsolutePath)
      assert(df.schema === StructType.fromDDL("events variant"))
      checkAnswer(df.selectExpr("to_json(events)"), Seq(Row("""{"a":1}"""), Row("""{"a":2}""")))
      // A user-specified schema may name the variant column differently from the array field.
      checkAnswer(
        spark.read.format("json").option("explodeEmbeddedArray", "events")
          .schema("record variant")
          .load(dir.getAbsolutePath).selectExpr("to_json(record)"),
        Seq(Row("""{"a":1}"""), Row("""{"a":2}""")))
    }
  }

  test("equivalent to reading the records as ndjson with singleVariantColumn") {
    withTempDir { dir =>
      val records = (0 until 1000).map { i =>
        s"""{"id": $i, "name": "event-$i", "values": [$i, {"nested": "str,[\\"$i"}]}"""
      }
      val embeddedDir = new File(dir, "embedded")
      val ndjsonDir = new File(dir, "ndjson")
      Utils.createDirectory(embeddedDir)
      Utils.createDirectory(ndjsonDir)
      // Records in the array are separated by newlines, and some records span multiple lines.
      val arrayBody = records.map(_.replace(", ", ",\n  ")).mkString(",\n")
      writeFile(embeddedDir, "file.json", s"""{"Records": [\n$arrayBody\n]}""")
      writeFile(ndjsonDir, "file.json", records.mkString("\n"))

      checkAnswer(
        spark.read.format("json").option("explodeEmbeddedArray", "Records")
          .load(embeddedDir.getAbsolutePath).selectExpr("to_json(Records)"),
        spark.read.format("json").option("singleVariantColumn", "record")
          .load(ndjsonDir.getAbsolutePath).selectExpr("to_json(record)").collect().toSeq)
    }
  }

  test("malformed records") {
    withTempDir { dir =>
      writeFile(dir, "file.json", """{"Records": [{"a": 1}, 12ab, {"b": 2}]}""")

      // PERMISSIVE (default) with a corrupt record column.
      checkAnswer(
        spark.read.format("json").option("explodeEmbeddedArray", "Records")
          .schema("record variant, _corrupt_record string")
          .load(dir.getAbsolutePath)
          .selectExpr("to_json(record)", "_corrupt_record"),
        Seq(Row("""{"a":1}""", null), Row(null, "12ab"), Row("""{"b":2}""", null)))

      // DROPMALFORMED drops the malformed record.
      checkAnswer(
        spark.read.format("json").option("explodeEmbeddedArray", "Records")
          .option("mode", "DROPMALFORMED")
          .load(dir.getAbsolutePath).selectExpr("to_json(Records)"),
        Seq(Row("""{"a":1}"""), Row("""{"b":2}""")))

      // FAILFAST fails the query.
      val e = intercept[Exception] {
        spark.read.format("json").option("explodeEmbeddedArray", "Records")
          .option("mode", "FAILFAST")
          .load(dir.getAbsolutePath).collect()
      }
      assert(Utils.exceptionString(e).contains("MALFORMED_RECORD_IN_PARSING"))
    }
  }

  test("truncated file") {
    withTempDir { dir =>
      writeFile(dir, "file.json", """{"Records": [{"a": 1}, {"b": """)
      checkAnswer(
        spark.read.format("json").option("explodeEmbeddedArray", "Records")
          .schema("record variant, _corrupt_record string")
          .load(dir.getAbsolutePath)
          .selectExpr("to_json(record)", "_corrupt_record"),
        Seq(Row("""{"a":1}""", null), Row(null, """{"b": """)))
    }
  }

  test("files without a matching array field yield no rows") {
    withTempDir { dir =>
      writeFile(dir, "empty.json", "")
      writeFile(dir, "no_records.json", """{"a": 1}""")
      writeFile(dir, "ndjson.json", "{\"a\": 1}\n{\"b\": 2}")
      writeFile(dir, "empty_records.json", """{"Records": []}""")
      writeFile(dir, "other_array.json", """{"events": [{"a": 1}]}""")
      assert(
        spark.read.format("json").option("explodeEmbeddedArray", "Records")
          .load(dir.getAbsolutePath).count() === 0)
    }
  }

  test("compressed file") {
    withTempDir { dir =>
      writeGzipFile(dir, "file.json.gz", documentContent)
      checkAnswer(
        spark.read.format("json").option("explodeEmbeddedArray", "Records")
          .load(dir.getAbsolutePath).selectExpr("to_json(Records)"),
        expectedRecords)
    }
  }

  test("multiple files and partition columns") {
    withTempDir { dir =>
      Utils.createDirectory(new File(dir, "p=1"))
      Utils.createDirectory(new File(dir, "p=2"))
      writeFile(dir, "p=1/file.json", """{"Records": [{"a": 1}, {"a": 2}]}""")
      writeFile(dir, "p=2/file.json", """{"Records": [{"a": 3}]}""")
      checkAnswer(
        spark.read.format("json").option("explodeEmbeddedArray", "Records")
          .load(dir.getAbsolutePath).selectExpr("p", "to_json(Records)"),
        Seq(Row(1, """{"a":1}"""), Row(1, """{"a":2}"""), Row(2, """{"a":3}""")))
    }
  }

  test("multiLine option does not interfere") {
    withTempDir { dir =>
      writeFile(dir, "file.json", documentContent)
      checkAnswer(
        spark.read.format("json").option("explodeEmbeddedArray", "Records")
          .option("multiLine", "true")
          .load(dir.getAbsolutePath).selectExpr("to_json(Records)"),
        expectedRecords)
    }
  }

  test("streaming read") {
    withTempDir { dir =>
      writeFile(dir, "file.json", documentContent)
      withTempDir { streamDir =>
        val stream = spark.readStream.format("json")
          .option("explodeEmbeddedArray", "Records")
          .load(dir.getCanonicalPath)
          .selectExpr("to_json(Records) as record")
          .writeStream
          .option("checkpointLocation", streamDir.getCanonicalPath + "/checkpoint")
          .format("parquet")
          .start(streamDir.getCanonicalPath + "/output")
        try {
          stream.processAllAvailable()
        } finally {
          stream.stop()
        }
        checkAnswer(
          spark.read.format("parquet").load(streamDir.getCanonicalPath + "/output"),
          expectedRecords)
      }
    }
  }

  test("user-specified schema constraints") {
    withTempDir { dir =>
      val file = writeFile(dir, "file.json", """{"Records": [{"a": 1}]}""")

      // These are valid. The variant column may have any name.
      for ((schema, options) <- Seq(
        ("record variant", Map[String, String]()),
        ("other variant", Map[String, String]()),
        ("record variant, _corrupt_record string", Map[String, String]()),
        ("c string, record variant", Map("columnNameOfCorruptRecord" -> "c"))
      )) {
        spark.read.options(Map("explodeEmbeddedArray" -> "Records") ++ options)
          .schema(schema).json(file.getAbsolutePath).collect()
      }

      // These are invalid.
      for ((schema, options) <- Seq(
        ("record int", Map[String, String]()),
        ("_corrupt_record variant", Map[String, String]()),
        ("a variant, b variant", Map[String, String]()),
        ("record variant, c string", Map[String, String]()),
        ("record variant, _corrupt_record string, c string", Map[String, String]())
      )) {
        checkError(
          exception = intercept[AnalysisException] {
            spark.read.options(Map("explodeEmbeddedArray" -> "Records") ++ options)
              .schema(schema).json(file.getAbsolutePath).collect()
          },
          condition = "INVALID_EXPLODE_EMBEDDED_ARRAY_SCHEMA",
          parameters = Map("schema" -> ("\"" + StructType.fromDDL(schema).sql + "\"")))
      }
    }
  }

  test("conflicting options") {
    withTempDir { dir =>
      writeFile(dir, "file.json", """{"Records": [{"a": 1}]}""")
      checkError(
        exception = intercept[AnalysisException] {
          spark.read.format("json")
            .option("explodeEmbeddedArray", "Records")
            .option("singleVariantColumn", "var")
            .load(dir.getAbsolutePath).collect()
        },
        condition = "EXPLODE_EMBEDDED_ARRAY_CONFLICTING_OPTION",
        parameters = Map("option" -> "singleVariantColumn"))
    }
  }

  test("rejected in from_json and json(Dataset[String])") {
    import testImplicits._
    checkError(
      exception = intercept[AnalysisException] {
        spark.read.option("explodeEmbeddedArray", "Records")
          .json(Seq("""{"Records": [{"a": 1}]}""").toDS())
      },
      condition = "EXPLODE_EMBEDDED_ARRAY_UNSUPPORTED_USAGE",
      parameters = Map("usage" -> "the json(Dataset[String]) API"))
    checkError(
      exception = intercept[AnalysisException] {
        spark.sql(
          """SELECT from_json('{"a": 1}', 'a INT', map('explodeEmbeddedArray', 'Records'))""")
          .collect()
      },
      condition = "EXPLODE_EMBEDDED_ARRAY_UNSUPPORTED_USAGE",
      parameters = Map("usage" -> "the from_json function"))
  }
}

class ExplodeEmbeddedArrayJsonV1Suite extends ExplodeEmbeddedArrayJsonSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "json")
}

class ExplodeEmbeddedArrayJsonV2Suite extends ExplodeEmbeddedArrayJsonSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
