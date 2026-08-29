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

package org.apache.spark.sql.execution.planmerging

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that subplan merging does not widen the set of columns a V1 file scan reads when the rows
 * that scan returns depend on that set.
 *
 * Every test asserts the number of `FileSourceScanExec` nodes as well as the rows, so that a
 * decline is attributed to the merge being declined rather than inferred from the values, and
 * finding a `FileSourceScanExec` at all is what pins the read to the V1 path.
 */
class FileSourceV1PlanMergingSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.USE_V1_SOURCE_LIST, "avro,csv,json,kafka,orc,parquet,text,xml")
    .set(SQLConf.IGNORE_CORRUPT_FILES, false)
    .set(SQLConf.IGNORE_MISSING_FILES, false)
    .set(SQLConf.SUBQUERY_REUSE_ENABLED, true)
    // Off so that the scans are in the executed plan rather than behind an AdaptiveSparkPlanExec.
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED, false)

  private val csvRows = "0,0\n1,10\n2,BAD\n3,30\n4,40"

  private val jsonRows = Seq(
    """{"a":0,"b":0}""",
    """{"a":1,"b":10}""",
    """{"a":2,"b":"BAD"}""",
    """{"a":3,"b":30}""",
    """{"a":4,"b":40}""").mkString("\n")

  private val xmlRows = Seq(
    "<rows>",
    "<row><a>0</a><b>0</b></row>",
    "<row><a>1</a><b>10</b></row>",
    "<row><a>2</a><b>BAD</b></row>",
    "<row><a>3</a><b>30</b></row>",
    "<row><a>4</a><b>40</b></row>",
    "</rows>").mkString("\n")

  /** Writes one file of `content` into `dir` and returns the directory to read back. */
  private def writeFile(dir: File, name: String, content: String): String = {
    Files.write(new File(dir, name).toPath, content.getBytes(StandardCharsets.UTF_8))
    dir.getCanonicalPath
  }

  /**
   * Runs `query` and returns its rows together with the number of V1 file scans in the plan, which
   * is one when the two subqueries share a scan and two when they do not. Collected first, because
   * the subquery plans are not in the executed plan until the query has run.
   */
  private def runCountingScans(query: String): (Array[Row], Int) = {
    val df = sql(query)
    val rows = df.collect()
    (rows, fileScans(df).size)
  }

  private def fileScans(df: DataFrame): Seq[FileSourceScanExec] =
    df.queryExecution.executedPlan.collectWithSubqueries { case s: FileSourceScanExec => s }

  Seq(
    ("csv", "data.csv", csvRows, Map.empty[String, String]),
    ("json", "data.json", jsonRows, Map.empty[String, String]),
    ("xml", "data.xml", xmlRows, Map("rowTag" -> "row"))
  ).foreach { case (format, fileName, content, extraOptions) =>
    test(s"SPARK-59107: $format DROPMALFORMED keeps a row malformed only in the other column") {
      withTempDir { dir =>
        val path = writeFile(dir, fileName, content)
        withTempView("t") {
          spark.read.schema("a long, b long").option("mode", "DROPMALFORMED")
            .options(extraOptions).format(format).load(path).createOrReplaceTempView("t")
          val (rows, scans) = runCountingScans(
            "SELECT (SELECT sum(a) FROM t), (SELECT sum(b) FROM t)")
          // A scan of a alone parses no b, so the row malformed in b is not dropped for sum(a).
          assert(rows === Array(Row(10L, 80L)))
          assert(scans === 2, "the two subqueries must not share a scan")
        }
      }
    }
  }
  test("SPARK-59107: PERMISSIVE does not populate the corrupt-record column of a clean row") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.csv", csvRows)
      withTempView("t") {
        spark.read.schema("a long, b long, _corrupt_record string")
          .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record")
          .csv(path).createOrReplaceTempView("t")
        val (rows, scans) = runCountingScans(
          "SELECT (SELECT count(_corrupt_record) FROM t WHERE a >= 0), " +
            "(SELECT sum(b) FROM t WHERE a >= 0)")
        assert(rows === Array(Row(0L, 80L)))
        assert(scans === 2, "the two subqueries must not share a scan")
      }
    }
  }

  test("SPARK-59107: FAILFAST does not fail on a short row the narrower scan accepted") {
    withTempDir { dir =>
      // One row carries fewer tokens than the schema has columns, which only a scan reading both
      // columns trips.
      val path = writeFile(dir, "data.csv", "0,0\n1,10\n2\n3,30\n4,40")
      withTempView("t") {
        spark.read.schema("a long, b long").option("mode", "FAILFAST")
          .csv(path).createOrReplaceTempView("t")
        val (rows, scans) = runCountingScans(
          "SELECT (SELECT sum(a) FROM t), (SELECT sum(b) FROM t)")
        assert(rows === Array(Row(10L, 80L)))
        assert(scans === 2, "the two subqueries must not share a scan")
      }
    }
  }
  Seq(SQLConf.IGNORE_CORRUPT_FILES, SQLConf.IGNORE_MISSING_FILES).foreach { conf =>
    test(s"SPARK-59107: a read that is not strict does not share a scan (${conf.key})") {
      withTempDir { dir =>
        val path = new File(dir, "data").getCanonicalPath
        spark.range(0, 10).selectExpr("id AS a", "id * 2 AS b").write.parquet(path)
        // The view is built outside the configuration scope: the gate is evaluated per merge, so a
        // relation built before the configuration was set still answers for the read that runs.
        withTempView("t") {
          spark.read.parquet(path).createOrReplaceTempView("t")
          withSQLConf(conf.key -> "true") {
            val (rows, scans) = runCountingScans(
              "SELECT (SELECT sum(a) FROM t), (SELECT sum(b) FROM t)")
            assert(rows === Array(Row(45L, 90L)))
            assert(scans === 2, "the two subqueries must not share a scan")
          }
        }
      }
    }
  }

  test("SPARK-59107: ignoreCorruptFiles does not drop rows a narrower scan returned") {
    withTempDir { dir =>
      val path = new File(dir, "data").getCanonicalPath
      // b is written as a string and read as a long, so the reader fails only when it reads b.
      spark.range(0, 10).selectExpr("id AS a", "cast(id AS string) AS b").write.parquet(path)
      withTempView("t") {
        spark.read.schema("a long, b long").parquet(path).createOrReplaceTempView("t")
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
          val (rows, scans) = runCountingScans(
            "SELECT (SELECT sum(a) FROM t), (SELECT count(b) FROM t)")
          // sum(a) touches only healthy data. A shared scan would fail on b, and that swallowed
          // failure would drop the rest of the file's rows, leaving sum(a) null.
          assert(rows === Array(Row(45L, 0L)))
          assert(scans === 2, "the two subqueries must not share a scan")
        }
      }
    }
  }

  test("SPARK-59107: a self join reads the relation twice, and each read counts on its own") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.csv", "0,0,0\n1,10,1\n2,BAD,2\n3,30,3\n4,40,4")
      withTempView("t") {
        spark.read.schema("a long, b long, k long").option("mode", "DROPMALFORMED")
          .csv(path).createOrReplaceTempView("t")
        // Both subqueries read the relation twice. The right side reads the same columns in both,
        // so only the left side differs, and a check that kept one column set per relation rather
        // than one per occurrence would compare the right sides and merge.
        val (rows, scans) = runCountingScans(
          "SELECT (SELECT count(t1.a) + sum(t2.b) FROM t t1 LEFT JOIN t t2 ON t1.k = t2.k), " +
            "(SELECT count(t1.b) + sum(t2.b) FROM t t1 LEFT JOIN t t2 ON t1.k = t2.k)")
        // Merging the left legs would read b for the first subquery too, dropping the row that is
        // malformed in b and answering 84 for it.
        assert(rows === Array(Row(85L, 84L)))
        // Three scans: the first subquery's two legs read different columns, and the second
        // subquery's two legs read the same columns so they share one.
        assert(scans === 3, "the two subqueries must not share a scan")
      }
    }
  }

  test("SPARK-59107: parquet subqueries that project different columns still share a scan") {
    withTempDir { dir =>
      val path = new File(dir, "data").getCanonicalPath
      spark.range(0, 10).selectExpr("id AS a", "id * 2 AS b").write.parquet(path)
      withTempView("t") {
        spark.read.parquet(path).createOrReplaceTempView("t")
        val (rows, scans) = runCountingScans(
          "SELECT (SELECT sum(a) FROM t), (SELECT sum(b) FROM t)")
        assert(rows === Array(Row(45L, 90L)))
        assert(scans === 1, "a format that decodes a row from the columns asked for still merges")
      }
    }
  }

  test("SPARK-59107: csv subqueries that read the same columns still share a scan") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.csv", "0,0\n1,10\n2,20\n3,30\n4,40")
      withTempView("t") {
        spark.read.schema("a long, b long").csv(path).createOrReplaceTempView("t")
        val (rows, scans) = runCountingScans(
          "SELECT (SELECT sum(a) FROM t WHERE b > 0), (SELECT count(a) FROM t WHERE b > 0)")
        // Both subqueries read a and b, so sharing one scan reads no more than either did.
        assert(rows === Array(Row(10L, 4L)))
        assert(scans === 1, "reads of the same columns are still merged")
      }
    }
  }
}
