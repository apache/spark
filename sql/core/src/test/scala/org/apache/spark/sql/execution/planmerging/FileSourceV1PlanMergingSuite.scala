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
 * Every test asserts the columns each scan in the plan reads, so that a decline is attributed to
 * the merge being declined rather than inferred from the values, and finding a `FileSourceScanExec`
 * at all is what pins the read to the V1 path. Most tests assert the rows as well; in the two that
 * only flip `ignoreCorruptFiles` or `ignoreMissingFiles` the rows come back the same either way, so
 * there the columns are the whole evidence.
 */
class FileSourceV1PlanMergingSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.USE_V1_SOURCE_LIST, "avro,csv,json,kafka,orc,parquet,text,xml")
    .set(SQLConf.IGNORE_CORRUPT_FILES, false)
    .set(SQLConf.IGNORE_MISSING_FILES, false)
    .set(SQLConf.SUBQUERY_REUSE_ENABLED, true)
    // Off because `AdaptiveSparkPlanExec` is a leaf node, so with it on the scans underneath it are
    // not reachable from the executed plan.
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
   * The columns each V1 file scan of `df`'s plan reads, one entry per scan, each sorted and the
   * entries sorted too, so that an assertion does not depend on plan order. Two entries with a
   * column each mean the two subqueries kept their own scans, and one entry holding both columns
   * means they shared one, which reuse cannot produce because it only replaces a scan that reads
   * the same columns. A replaced scan is absent from the list, since both `ReusedSubqueryExec` and
   * `ReusedExchangeExec` are leaf nodes, which is why the self join below shows three scans of the
   * four its plan contains.
   */
  private def scanColumns(df: DataFrame): Seq[Seq[String]] =
    df.queryExecution.executedPlan
      .collectWithSubqueries { case s: FileSourceScanExec => s }
      .map(_.requiredSchema.fieldNames.sorted.toSeq)
      .sortBy(_.mkString(","))

  // One test per format rather than `gridTest`, so that the format reads in the middle of the name.
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
          val df = sql("SELECT (SELECT sum(a) FROM t), (SELECT sum(b) FROM t)")
          // A scan of a alone parses no b, so the row malformed in b is not dropped for sum(a).
          checkAnswer(df, Row(10L, 80L))
          assert(scanColumns(df) === Seq(Seq("a"), Seq("b")))
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
        val df = sql(
          "SELECT (SELECT count(_corrupt_record) FROM t WHERE a >= 0), " +
            "(SELECT sum(b) FROM t WHERE a >= 0)")
        checkAnswer(df, Row(0L, 80L))
        assert(scanColumns(df) === Seq(Seq("_corrupt_record", "a"), Seq("a", "b")))
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
        val df = sql("SELECT (SELECT sum(a) FROM t), (SELECT sum(b) FROM t)")
        checkAnswer(df, Row(10L, 80L))
        assert(scanColumns(df) === Seq(Seq("a"), Seq("b")))
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
            val df = sql("SELECT (SELECT sum(a) FROM t), (SELECT sum(b) FROM t)")
            // The rows are the same either way here; the columns are what says it declined.
            checkAnswer(df, Row(45L, 90L))
            assert(scanColumns(df) === Seq(Seq("a"), Seq("b")))
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
          val df = sql("SELECT (SELECT sum(a) FROM t), (SELECT count(b) FROM t)")
          // sum(a) touches only healthy data. A shared scan would fail on b, and that swallowed
          // failure would drop the rest of the file's rows, leaving sum(a) null.
          checkAnswer(df, Row(45L, 0L))
          assert(scanColumns(df) === Seq(Seq("a"), Seq("b")))
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
        val df = sql(
          "SELECT (SELECT count(t1.a) + sum(t2.b) FROM t t1 LEFT JOIN t t2 ON t1.k = t2.k), " +
            "(SELECT count(t1.b) + sum(t2.b) FROM t t1 LEFT JOIN t t2 ON t1.k = t2.k)")
        // Merging the left legs would read b for the first subquery too, dropping the row that is
        // malformed in b and answering 84 for it.
        checkAnswer(df, Row(85L, 84L))
        // Three of the four scans are visible: two subqueries reading (a, k) and (b, k), and one
        // more (b, k), the fourth having been replaced by reuse of an identical scan.
        assert(scanColumns(df) === Seq(Seq("a", "k"), Seq("b", "k"), Seq("b", "k")))
      }
    }
  }

  test("SPARK-59107: a merge that rebuilds a projection does not widen the read either") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.csv", csvRows)
      withTempView("t") {
        spark.read.schema("a long, b long").option("mode", "DROPMALFORMED")
          .csv(path).createOrReplaceTempView("t")
        // Both subqueries read a alone, and filter propagation, which is on by default, merges
        // them by rebuilding the projection above the relation. That rebuilt projection carries the
        // relation's full output, but the `ColumnPruning` rerun after this rule narrows it again,
        // so the shared scan reads only a and the answers do not change.
        val df = sql(
          "SELECT (SELECT sum(x) FROM (SELECT a * 2 AS x FROM t WHERE a > 1)), " +
            "(SELECT sum(a) FROM t)")
        checkAnswer(df, Row(18L, 10L))
        assert(scanColumns(df) === Seq(Seq("a")))
      }
    }
  }

  test("SPARK-59107: a third subquery is compared against what the merged pair read") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.csv", csvRows)
      withTempView("t") {
        spark.read.schema("a long, b long").option("mode", "DROPMALFORMED")
          .csv(path).createOrReplaceTempView("t")
        // The first two subqueries merge on a, and the merged plan carries the relation's full
        // output until `ColumnPruning` runs again. The third reads a and b, so it must be compared
        // against what the first two read rather than against the merged plan, or it would join
        // them and widen their read.
        val df = sql(
          "SELECT (SELECT sum(x) FROM (SELECT a * 2 AS x FROM t WHERE a > 1)), " +
            "(SELECT sum(a) FROM t), (SELECT sum(a + b) FROM t)")
        checkAnswer(df, Row(18L, 10L, 88L))
        assert(scanColumns(df) === Seq(Seq("a"), Seq("a", "b")))
      }
    }
  }

  test("SPARK-59107: a fourth subquery joins the entry the refused third one started") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.csv", csvRows)
      withTempView("t") {
        spark.read.schema("a long, b long").option("mode", "DROPMALFORMED")
          .csv(path).createOrReplaceTempView("t")
        // The third subquery is refused by the entry the first two share, so it opens one of its
        // own, and the fourth has to find that entry rather than stop at the refusal.
        val df = sql(
          "SELECT (SELECT sum(x) FROM (SELECT a * 2 AS x FROM t WHERE a > 1)), " +
            "(SELECT sum(a) FROM t), (SELECT sum(a + b) FROM t), (SELECT count(a + b) FROM t)")
        checkAnswer(df, Row(18L, 10L, 88L, 4L))
        assert(scanColumns(df) === Seq(Seq("a"), Seq("a", "b")))
      }
    }
  }

  test("SPARK-59107: a third subquery reading the same columns still shares the scan") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.csv", csvRows)
      withTempView("t") {
        spark.read.schema("a long, b long").option("mode", "DROPMALFORMED")
          .csv(path).createOrReplaceTempView("t")
        val df = sql(
          "SELECT (SELECT sum(a) FROM t), (SELECT count(a) FROM t), (SELECT max(a) FROM t)")
        checkAnswer(df, Row(10L, 5L, 4L))
        assert(scanColumns(df) === Seq(Seq("a")))
      }
    }
  }

  test("SPARK-59107: parquet subqueries that project different columns still share a scan") {
    withTempDir { dir =>
      val path = new File(dir, "data").getCanonicalPath
      spark.range(0, 10).selectExpr("id AS a", "id * 2 AS b").write.parquet(path)
      withTempView("t") {
        spark.read.parquet(path).createOrReplaceTempView("t")
        val df = sql("SELECT (SELECT sum(a) FROM t), (SELECT sum(b) FROM t)")
        checkAnswer(df, Row(45L, 90L))
        assert(scanColumns(df) === Seq(Seq("a", "b")),
          "a format that decodes a row from the columns asked for still merges")
      }
    }
  }

  test("SPARK-59107: csv subqueries that read the same columns still share a scan") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.csv", "0,0\n1,10\n2,20\n3,30\n4,40")
      withTempView("t") {
        spark.read.schema("a long, b long").csv(path).createOrReplaceTempView("t")
        val df = sql(
          "SELECT (SELECT sum(a) FROM t WHERE b > 0), (SELECT count(a) FROM t WHERE b > 0)")
        // Both subqueries read a and b, so sharing one scan reads no more than either did.
        checkAnswer(df, Row(10L, 4L))
        assert(scanColumns(df) === Seq(Seq("a", "b")))
      }
    }
  }

  test("SPARK-59107: identical subqueries over such a read still share a scan") {
    withTempDir { dir =>
      val path = writeFile(dir, "data.csv", csvRows)
      withTempView("t") {
        spark.read.schema("a long, b long").option("mode", "DROPMALFORMED")
          .csv(path).createOrReplaceTempView("t")
        // End to end this shape runs one scan whatever the merger decides, because subquery reuse
        // collapses two identical subqueries on its own. Here to pin that it stays that way.
        val df = sql("SELECT (SELECT sum(b) FROM t), (SELECT sum(b) FROM t) + 1")
        checkAnswer(df, Row(80L, 81L))
        assert(scanColumns(df) === Seq(Seq("b")))
      }
    }
  }

  test("SPARK-59107: a partition column reference does not count as a column read") {
    withTempDir { dir =>
      val path = new File(dir, "data").getCanonicalPath
      spark.range(0, 8).selectExpr("id AS a", "id % 2 AS p", "id % 4 AS q")
        .write.partitionBy("p", "q").csv(path)
      withTempView("t") {
        spark.read.schema("a long, p long, q long").csv(path).createOrReplaceTempView("t")
        // q comes from the path rather than from the file, so the two subqueries read the same
        // column of it, a, and the merge is allowed even though this is a csv relation.
        val df = sql(
          "SELECT (SELECT sum(a) FROM t WHERE p = 1), (SELECT sum(a + q) FROM t WHERE p = 1)")
        checkAnswer(df, Row(16L, 24L))
        assert(scanColumns(df) === Seq(Seq("a")))
      }
    }
  }
}
