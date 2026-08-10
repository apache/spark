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

package org.apache.spark.sql.execution.datasources.fwf

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.fwf.FixedWidthOptions
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class FixedWidthSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private def pad(s: String, width: Int): String = s.padTo(width, ' ').take(width)

  private def fixedWidthLine(fields: Seq[String], widths: Seq[Int]): String =
    fields.zip(widths).map { case (f, w) => pad(f, w) }.mkString

  private val widths = Seq(4, 10, 6)
  private val header = fixedWidthLine(Seq("id", "name", "score"), widths)
  private val row1 = fixedWidthLine(Seq("1", "Alice", "95.5"), widths)
  private val row2 = fixedWidthLine(Seq("2", "Bob", "88.0"), widths)

  private def writeFile(dir: File, name: String, lines: Seq[String]): String = {
    val path = new File(dir, name)
    Files.write(path.toPath, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
    path.getCanonicalPath
  }

  test("read with explicit widths and header") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .load(path)

      assert(df.schema.fieldNames === Array("id", "name", "score"))
      assert(df.schema("id").dataType === IntegerType)
      assert(df.schema("name").dataType === StringType)
      assert(df.schema("score").dataType === DoubleType)
      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
    }
  }

  test("read with explicit colspecs, no header") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(row1, row2))
      val df = spark.read
        .format("fwf")
        .option("colspecs", "0-4,4-14,14-20")
        .load(path)

      assert(df.schema.fieldNames === Array("_c0", "_c1", "_c2"))
      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
    }
  }

  test("colspecs=\"infer\" (the default) detects column boundaries from sampled data") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      val df = spark.read
        .format("fwf")
        .option("header", "true")
        .load(path)

      assert(df.schema.fieldNames === Array("id", "name", "score"))
      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
    }
  }

  test("comment lines are excluded from both colspec inference and reading") {
    withTempDir { dir =>
      val commentLine = "# a totally different shape of line that would skew inference"
      val path = writeFile(dir, "data.txt", Seq(commentLine, header, row1, commentLine, row2))
      val df = spark.read
        .format("fwf")
        .option("header", "true")
        .option("comment", "#")
        .load(path)

      assert(df.schema.fieldNames === Array("id", "name", "score"))
      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
    }
  }

  test("nullValue option") {
    withTempDir { dir =>
      val rowWithNull = fixedWidthLine(Seq("3", "NA", "NA"), widths)
      val path = writeFile(dir, "data.txt", Seq(header, row1, rowWithNull))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .option("nullValue", "NA")
        .load(path)

      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(3, null, null)))
    }
  }

  // A field that cannot be cast to its inferred type (score is DoubleType, "oops" isn't a
  // number), not merely a short/truncated line -- a line clamped by `sliceLine` to fewer
  // characters than the last colspec still yields the right *number* of tokens (just some
  // empty), so it parses cleanly to a null rather than tripping the malformed-row path.
  //
  // An explicit schema is required here: with schema *inference* on (the default), the
  // malformed row's "oops" would itself be part of the sample, and CSVInferSchema would
  // correctly widen the inferred type of `score` to StringType to accommodate it -- at which
  // point "oops" is a perfectly valid string and there is no conversion failure left to test.
  private def malformedRow: String = fixedWidthLine(Seq("9", "Zed", "oops"), widths)
  private val idNameScoreSchema = new StructType()
    .add("id", IntegerType).add("name", StringType).add("score", DoubleType)

  test("a field that fails to convert is nulled out in PERMISSIVE mode (the default)") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, malformedRow))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .schema(idNameScoreSchema)
        .load(path)

      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(9, "Zed", null)))
    }
  }

  test("a field that fails to convert drops its row in DROPMALFORMED mode") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, malformedRow))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(idNameScoreSchema)
        .load(path)

      checkAnswer(df, Seq(Row(1, "Alice", 95.5)))
    }
  }

  test("a field that fails to convert throws in FAILFAST mode") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, malformedRow))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(idNameScoreSchema)
        .load(path)

      intercept[SparkException](df.collect())
    }
  }

  private def causeChainContains(t: Throwable, needle: String): Boolean = {
    var cur = t
    while (cur != null) {
      if (cur.getMessage != null && cur.getMessage.contains(needle)) return true
      cur = cur.getCause
    }
    false
  }

  test("writing requires explicit widths or colspecs") {
    withTempDir { dir =>
      val out = new File(dir, "out").getCanonicalPath
      val df = Seq((1, "Alice", 95.5), (2, "Bob", 88.0)).toDF("id", "name", "score")
      val e = intercept[Exception] {
        df.write.format("fwf").save(out)
      }
      assert(causeChainContains(e, "colspecs"))
    }
  }

  test("write and read back round-trips with explicit widths") {
    withTempDir { dir =>
      val out = new File(dir, "out").getCanonicalPath
      val df = Seq((1, "Alice", 95.5), (2, "Bob", 88.0)).toDF("id", "name", "score")
      df.coalesce(1).write.format("fwf").option("widths", "4,10,6").save(out)

      val readBack = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .schema(df.schema)
        .load(out)

      checkAnswer(readBack, df)
    }
  }

  test("colspecs=\"infer\" combined with a user-supplied schema is rejected") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      val userSchema = new StructType()
        .add("id", IntegerType).add("name", StringType).add("score", DoubleType)
      val df = spark.read
        .format("fwf")
        .option("header", "true")
        .schema(userSchema)
        .load(path)

      intercept[Exception](df.collect())
    }
  }

  test("SQL: CREATE TEMPORARY VIEW ... USING fwf") {
    // Mirrors CSVSuite's own DDL tests (e.g. "DDL test with schema"): `path` as an OPTIONS entry
    // on a temporary view, rather than a `LOCATION` clause on a `CREATE TABLE`. The latter routes
    // through `CreateDataSourceTableCommand` -> `InMemoryFileIndex`, which -- for any file format,
    // not anything FWF-specific -- walks a path's ancestors on this Windows host looking for a
    // sibling `_spark_metadata` directory and hits a pre-existing `Path.getParent` edge case above
    // a drive root (`Can not create a Path from an empty string`); unrelated to what's under test.
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      withView("fwf_view") {
        sql(
          s"""
             |CREATE TEMPORARY VIEW fwf_view (id INT, name STRING, score DOUBLE)
             |USING fwf
             |OPTIONS (path "${path.replace("\\", "/")}", widths "4,10,6", header "true")
             |""".stripMargin.replaceAll("\n", " "))
        checkAnswer(spark.table("fwf_view"), Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
      }
    }
  }

  test("duplicate/blank header names are deduplicated like CSV's makeSafeHeader") {
    withTempDir { dir =>
      val dupHeader = fixedWidthLine(Seq("id", "id", ""), Seq(4, 4, 4))
      val dataRow = fixedWidthLine(Seq("1", "2", "3"), Seq(4, 4, 4))
      val path = writeFile(dir, "data.txt", Seq(dupHeader, dataRow))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,4,4")
        .option("header", "true")
        .load(path)

      assert(df.schema.fieldNames === Array("id0", "id1", "_c2"))
      checkAnswer(df, Seq(Row(1, 2, 3)))
    }
  }

  test("write and read back round-trips with header") {
    withTempDir { dir =>
      val out = new File(dir, "out").getCanonicalPath
      val df = Seq((1, "Alice", 95.5), (2, "Bob", 88.0)).toDF("id", "name", "score")
      df.coalesce(1).write.format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .save(out)

      val readBack = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .load(out)

      assert(readBack.schema.fieldNames === Array("id", "name", "score"))
      checkAnswer(readBack, df)
    }
  }

  test("skipRows as an integer skips a fixed number of rows from the start") {
    withTempDir { dir =>
      val junk = "not part of the data at all, just noise"
      val path = writeFile(dir, "data.txt", Seq(junk, header, row1, row2))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .option("skipRows", "1")
        .load(path)

      assert(df.schema.fieldNames === Array("id", "name", "score"))
      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
    }
  }

  test("skipRows as a list skips specific row numbers anywhere in the file") {
    withTempDir { dir =>
      val junk1 = "junk row A"
      val junk2 = "junk row B"
      val path = writeFile(dir, "data.txt", Seq(header, junk1, row1, junk2, row2))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .option("skipRows", "1,3")
        .load(path)

      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
    }
  }

  test("skipRows also excludes skipped rows from colspecs=\"infer\" sampling") {
    withTempDir { dir =>
      val junk = "###################"
      val path = writeFile(dir, "data.txt", Seq(junk, header, row1, row2))
      val df = spark.read
        .format("fwf")
        .option("header", "true")
        .option("skipRows", "1")
        .load(path)

      assert(df.schema.fieldNames === Array("id", "name", "score"))
      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
    }
  }

  test("selecting a subset of columns exercises column pruning correctly") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .load(path)

      checkAnswer(df.select("name"), Seq(Row("Alice"), Row("Bob")))
      checkAnswer(df.select("score", "id"), Seq(Row(95.5, 1), Row(88.0, 2)))
    }
  }

  test("column pruning down to a blank selected column, or to none at all, keeps every row") {
    // A row is only "blank" if its whole raw line is; whether it stays blank once pruned down to
    // the specific columns a query selects (here, none at all for count(), or just the one column
    // that happens to be empty for row3) must not change that.
    withTempDir { dir =>
      val row3 = fixedWidthLine(Seq("3", "Carol", ""), widths)
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2, row3))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .load(path)

      assert(df.count() === 3)
      checkAnswer(df.select("id"), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(df.select("score"), Seq(Row(95.5), Row(88.0), Row(null)))
    }
  }

  test("filtering on a column exercises filter pushdown correctly") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .load(path)

      checkAnswer(df.filter("id = 2").select("name"), Seq(Row("Bob")))
      checkAnswer(df.filter("score > 90.0"), Seq(Row(1, "Alice", 95.5)))

      withSQLConf("spark.sql.fwf.filterPushdown.enabled" -> "false") {
        checkAnswer(df.filter("id = 2").select("name"), Seq(Row("Bob")))
      }
    }
  }

  test("spark.read.fwf and df.write.fwf convenience methods") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      val df = spark.read.option("widths", "4,10,6").option("header", "true").fwf(path)
      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))

      val out = new File(dir, "out").getCanonicalPath
      df.coalesce(1).write.option("widths", "4,10,6").fwf(out)
      checkAnswer(
        spark.read.option("widths", "4,10,6").schema(df.schema).fwf(out),
        df)
    }
  }

  test("validate fixed-width Options") {
    assert(FixedWidthOptions.getAllOptions.size == 16)
    // Please add validation on any new fixed-width options here
    assert(FixedWidthOptions.isValidOption("colspecs"))
    assert(FixedWidthOptions.isValidOption("widths"))
    assert(FixedWidthOptions.isValidOption("inferNrows"))
    assert(FixedWidthOptions.isValidOption("header"))
    assert(FixedWidthOptions.isValidOption("comment"))
    assert(FixedWidthOptions.isValidOption("delimiter"))
    assert(FixedWidthOptions.isValidOption("nullValue"))
    assert(FixedWidthOptions.isValidOption("dateFormat"))
    assert(FixedWidthOptions.isValidOption("timestampFormat"))
    assert(FixedWidthOptions.isValidOption("timeZone"))
    assert(FixedWidthOptions.isValidOption("locale"))
    assert(FixedWidthOptions.isValidOption("mode"))
    assert(FixedWidthOptions.isValidOption("columnNameOfCorruptRecord"))
    assert(FixedWidthOptions.isValidOption("skipRows"))
    assert(FixedWidthOptions.isValidOption("encoding"))
    assert(FixedWidthOptions.isValidOption("charset"))
    // Please add validation on any new fixed-width options with alternative here
    assert(FixedWidthOptions.getAlternativeOption("encoding").contains("charset"))
    assert(FixedWidthOptions.getAlternativeOption("charset").contains("encoding"))
  }
}
