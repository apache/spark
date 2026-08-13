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

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.fwf.FixedWidthOptions
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.fwf.FixedWidthScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{EqualTo, Filter}
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

  private def malformedRow: String = fixedWidthLine(Seq("9", "Zed", "oops"), widths)
  private val idNameScoreSchema = new StructType()
    .add("id", IntegerType).add("name", StringType).add("score", DoubleType)

  private def writeFile(dir: File, name: String, lines: Seq[String]): String = {
    val path = new File(dir, name)
    Files.write(path.toPath, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
    path.getCanonicalPath
  }

  private def illegalArg(t: Throwable): SparkIllegalArgumentException = {
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)
      .collectFirst { case e: SparkIllegalArgumentException => e }
      .getOrElse(fail(s"No SparkIllegalArgumentException in: $t"))
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

  test("infer colspecs") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      val df = spark.read.format("fwf").option("header", "true").load(path)

      assert(df.schema.fieldNames === Array("id", "name", "score"))
      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
    }
  }

  test("commented lines") {
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

  test("nullValue") {
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

  test("PERMISSIVE mode") {
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

  test("DROPMALFORMED mode") {
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

  test("columnNameOfCorruptRecord") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, malformedRow))
      val schemaWithCorrupt = idNameScoreSchema.add("_corrupt", StringType)
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .schema(schemaWithCorrupt)
        .load(path)

      checkAnswer(df, Seq(
        Row(1, "Alice", 95.5, null),
        Row(9, "Zed", null, "9 Zed oops")))
    }
  }

  test("FAILFAST mode") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, malformedRow))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(idNameScoreSchema)
        .load(path)

      val ex = intercept[SparkException](df.collect())
      checkErrorMatchPVals(
        exception = ex,
        condition = "FAILED_READ_FILE.NO_HINT",
        parameters = Map("path" -> s".*$path.*"))
      checkError(
        exception = ex.getCause.asInstanceOf[SparkException],
        condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
        parameters = Map(
          "badRecord" -> "[9,Zed,null]",
          "failFastMode" -> "FAILFAST"))
    }
  }

  test("writing requires explicit widths or colspecs") {
    withTempDir { dir =>
      val out = new File(dir, "out").getCanonicalPath
      val df = Seq((1, "Alice", 95.5), (2, "Bob", 88.0)).toDF("id", "name", "score")
      checkError(
        exception = illegalArg(intercept[Exception] {
          df.write.format("fwf").save(out)
        }),
        condition = "INVALID_FIXED_WIDTH_COLSPECS.REQUIRED_FOR_WRITE",
        parameters = Map.empty)
    }
  }

  test("write and read round-trip") {
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

  test("infer colspecs rejected with a user schema") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      val df = spark.read
        .format("fwf")
        .option("header", "true")
        .schema(idNameScoreSchema)
        .load(path)

      checkError(
        exception = illegalArg(intercept[Exception](df.collect())),
        condition = "INVALID_FIXED_WIDTH_COLSPECS.CANNOT_INFER_WITH_USER_SCHEMA",
        parameters = Map.empty)
    }
  }

  test("DDL test with schema") {
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

  test("duplicate and blank header names") {
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

  test("skipRows as an integer") {
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

  test("skipRows as a list") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, "junk row A", row1, "junk row B", row2))
      val df = spark.read
        .format("fwf")
        .option("widths", "4,10,6")
        .option("header", "true")
        .option("skipRows", "1,3")
        .load(path)

      checkAnswer(df, Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
    }
  }

  test("column pruning") {
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

  test("column pruning does not drop rows") {
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

  test("filters push down") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))

      def load(): DataFrame =
        spark.read.format("fwf").option("widths", "4,10,6").option("header", "true").load(path)

      def pushed(df: DataFrame): Array[Filter] = {
        df.queryExecution.sparkPlan.collectFirst {
          case b: BatchScanExec => b.scan.asInstanceOf[FixedWidthScan].pushedFilters
        }.get
      }

      val filtered = load().filter($"id" === 2)
      checkAnswer(filtered.select("name"), Seq(Row("Bob")))
      assert(pushed(filtered).exists {
        case EqualTo("id", 2) => true
        case _ => false
      })

      withSQLConf(SQLConf.FWF_FILTER_PUSHDOWN_ENABLED.key -> "false") {
        val disabled = load().filter($"id" === 2)
        checkAnswer(disabled.select("name"), Seq(Row("Bob")))
        assert(pushed(disabled).isEmpty)
      }
    }
  }

  test("spark.read.fwf") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.txt", Seq(header, row1, row2))
      checkAnswer(
        spark.read.option("widths", "4,10,6").option("header", "true").fwf(path),
        Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 88.0)))
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
