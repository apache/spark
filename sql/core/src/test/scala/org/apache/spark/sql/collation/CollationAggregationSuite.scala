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

package org.apache.spark.sql.collation

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.RewriteCollationAggregate
import org.apache.spark.sql.catalyst.expressions.CollationKey
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CollationAggregationSuite
  extends SharedSparkSession
  with AdaptiveSparkPlanHelper {

  private def usesSortAggregate(df: DataFrame): Boolean =
    collectFirst(df.queryExecution.executedPlan) { case _: SortAggregateExec => true }.nonEmpty

  private def usesHashAggregate(df: DataFrame): Boolean =
    collectFirst(df.queryExecution.executedPlan) { case _: HashAggregateExec => true }.nonEmpty

  private def usesObjectHashAggregate(df: DataFrame): Boolean =
    collectFirst(df.queryExecution.executedPlan) {
      case _: ObjectHashAggregateExec => true
    }.nonEmpty

  private def usesHashBasedAggregate(df: DataFrame): Boolean =
    usesHashAggregate(df) || usesObjectHashAggregate(df)

  private def withHelloTable(f: String => Unit): Unit = {
    val tblName = "grp_by_tbl"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('HELLO', 1), ('hello', 2), ('HeLlO', 3)")
      f(tblName)
    }
  }

  test("group by collated column uses hash aggregation when the key is not projected") {
    withHelloTable { tblName =>
      // The collated grouping key is normalized to its collation key, so the three case-variant
      // rows collapse into a single group and hash aggregation is used instead of a sort.
      val df = sql(s"SELECT COUNT(*) FROM $tblName GROUP BY c1")
      assert(usesHashAggregate(df))
      assert(!usesSortAggregate(df))
      checkAnswer(df, Seq(Row(3)))
    }
  }

  test("group by collated column uses object hash aggregation when the key is projected") {
    withHelloTable { tblName =>
      // The original grouping value is carried through the output via First, which yields a
      // non-mutable aggregation buffer, so object hash aggregation (not sort) is used.
      val df = sql(s"SELECT LOWER(c1) AS k, COUNT(*) AS cnt FROM $tblName GROUP BY c1")
      assert(usesObjectHashAggregate(df))
      assert(!usesSortAggregate(df))
      checkAnswer(df, Seq(Row("hello", 3)))

      // The projected key is one of the collation-equal representatives of the group.
      val representatives =
        sql(s"SELECT c1 FROM $tblName GROUP BY c1").collect().map(_.getString(0))
      assert(representatives.length == 1)
      assert(Set("HELLO", "hello", "HeLlO").contains(representatives.head))
    }
  }

  test("typed imperative aggregate uses object hash aggregation for collated grouping keys") {
    withHelloTable { tblName =>
      val df = sql(s"SELECT COLLECT_LIST(c2) AS list FROM $tblName GROUP BY c1")
      assert(usesObjectHashAggregate(df))
      assert(!usesSortAggregate(df))
      checkAnswer(df.selectExpr("array_sort(list)"), Seq(Row(Seq(1, 2, 3))))
    }
  }

  test("group by collated column merges case-variant keys across multiple groups") {
    val tblName = "grp_by_multi"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES " +
        "('Apple', 1), ('APPLE', 2), ('banana', 3), ('Banana', 4), ('cherry', 5)")

      val df = sql(s"SELECT LOWER(c1) AS k, COUNT(*) AS cnt FROM $tblName GROUP BY c1")
      assert(!usesSortAggregate(df))
      checkAnswer(df, Seq(Row("apple", 2), Row("banana", 2), Row("cherry", 1)))
    }
  }

  test("bare projected collated key is remapped for parent operators") {
    val tblName = "grp_by_order"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES " +
        "('Apple', 1), ('APPLE', 2), ('banana', 3), ('cherry', 4), ('CHERRY', 5)")

      // The bare grouping key `c1` is projected and gets a fresh expression id after the rewrite;
      // the enclosing Sort (ORDER BY c1) and Filter (HAVING) reference that output and must be
      // remapped, otherwise the plan fails validation or execution.
      val df = sql(
        s"""SELECT c1, COUNT(*) AS cnt FROM $tblName
           |GROUP BY c1 HAVING COUNT(*) > 1 ORDER BY c1""".stripMargin)
      assert(!usesSortAggregate(df))
      checkAnswer(df.selectExpr("LOWER(c1) AS k", "cnt"), Seq(Row("apple", 2), Row("cherry", 2)))
    }
  }

  test("duplicate projected collated grouping key does not fail plan validation") {
    withHelloTable { tblName =>
      // Both projections of the bare grouping key share one replacement, so a single old output
      // expression id does not map to two fresh ids.
      val rows = sql(s"SELECT c1, c1 FROM $tblName GROUP BY c1").collect()
      assert(rows.length == 1)
      assert(rows.head.getString(0) == rows.head.getString(1))
      assert(Set("HELLO", "hello", "HeLlO").contains(rows.head.getString(0)))
    }
  }

  test("projected collated grouping key preserves nullability") {
    // The rewrite carries the key via First, which is always nullable. It must not relax the
    // nullability of a non-nullable grouping key, so its output nullability should match the
    // non-rewritten (binary collation) path.
    val collated =
      sql("SELECT c1 FROM VALUES ('a' COLLATE UTF8_LCASE), ('A' COLLATE UTF8_LCASE) AS t(c1) " +
        "GROUP BY c1")
    val binary = sql("SELECT c1 FROM VALUES ('a'), ('A') AS t(c1) GROUP BY c1")
    assert(collated.schema.head.nullable == binary.schema.head.nullable)
  }

  test("first over collated column with a binary grouping key stays sort-based") {
    val tblName = "first_collated"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (k INT, name STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES (1, 'a'), (1, 'A'), (2, 'b')")

      // The grouping key is binary-stable and no collation normalization happens, so a user-written
      // first(collated) must not trip the object-hash preference for collated aggregations.
      val df = sql(s"SELECT k, FIRST(name) AS f FROM $tblName GROUP BY k")
      assert(usesSortAggregate(df))
      assert(!usesObjectHashAggregate(df))
    }
  }

  test("hash aggregation is used for struct grouping keys with collated fields") {
    val tblName = "struct_grp"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (s STRUCT<name: STRING COLLATE UTF8_LCASE, id: INT>) " +
        "USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES (named_struct('name', 'A', 'id', 1)), " +
        "(named_struct('name', 'a', 'id', 1)), (named_struct('name', 'b', 'id', 2))")

      val df = sql(s"SELECT COUNT(*) AS cnt FROM $tblName GROUP BY s")
      assert(!usesSortAggregate(df))
      // ('A', 1) and ('a', 1) are collation-equal, so they form a single group.
      checkAnswer(df, Seq(Row(2), Row(1)))
    }
  }

  test("mixed normalized and unnormalizable grouping keys use sort aggregation") {
    val tblName = "mixed_grp"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (s STRING COLLATE UTF8_LCASE, " +
        "m MAP<INT, STRING COLLATE UTF8_LCASE>) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('A', map(1, 'x')), ('a', map(1, 'X')), ('b', map(2, 'y'))")

      // `CollationKey` cannot normalize a map containing collated strings, so that grouping key
      // stays non-binary-stable. Object-hash aggregation must not be used, otherwise the collated
      // map values would be grouped by raw bytes.
      val df = sql(s"SELECT COUNT(*) AS cnt FROM $tblName GROUP BY s, m")
      assert(usesSortAggregate(df))
      assert(!usesObjectHashAggregate(df))
      // ('A', {1 -> 'x'}) and ('a', {1 -> 'X'}) are collation-equal in both keys -> one group.
      checkAnswer(df, Seq(Row(2), Row(1)))
    }
  }

  test("count distinct with a projected collated key uses object hash aggregation") {
    withHelloTable { tblName =>
      val df = sql(s"SELECT c1, COUNT(DISTINCT c2) AS d FROM $tblName GROUP BY c1")
      assert(usesObjectHashAggregate(df))
      assert(!usesSortAggregate(df))
      checkAnswer(df.selectExpr("LOWER(c1) AS k", "d"), Seq(Row("hello", 3)))
    }
  }

  test("multiple distinct aggregates with a projected collated key use object hash aggregation") {
    val tblName = "multi_distinct_projected_key"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT, c3 INT) " +
        "USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('A', 1, 10), ('a', 2, 10), ('A', 2, 20)")

      val df = sql(
        s"""SELECT c1, COUNT(DISTINCT c2) AS d2, COUNT(DISTINCT c3) AS d3
           |FROM $tblName GROUP BY c1""".stripMargin)
      assert(usesObjectHashAggregate(df))
      assert(!usesSortAggregate(df))
      checkAnswer(df.selectExpr("LOWER(c1) AS k", "d2", "d3"), Seq(Row("a", 2, 2)))
    }
  }

  test("rollup on a collated column groups collation-equal rows and adds a total") {
    val tblName = "rollup_grp"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, v INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('Apple', 1), ('APPLE', 2), ('banana', 3)")

      // ROLLUP builds an Expand whose Aggregate groups on the collated key plus a grouping id; the
      // collated key is normalized so the grouped rows still merge collation-equal values.
      val df = sql(s"SELECT LOWER(c1) AS k, COUNT(*) AS cnt FROM $tblName GROUP BY ROLLUP(c1)")
      assert(!usesSortAggregate(df))
      // Per-group: apple (Apple + APPLE) and banana; plus the grand total (c1 rolled up to NULL).
      checkAnswer(df, Seq(Row("apple", 2), Row("banana", 1), Row(null, 3)))
    }
  }

  test("object hash kill switch keeps collated aggregation correct") {
    withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
      withHelloTable { tblName =>
        // With object-hash disabled, a projected collated key falls back to sort aggregation but
        // stays collation-correct.
        val df = sql(s"SELECT LOWER(c1) AS k, COUNT(*) AS cnt FROM $tblName GROUP BY c1")
        assert(usesSortAggregate(df))
        assert(!usesObjectHashAggregate(df))
        checkAnswer(df, Seq(Row("hello", 3)))
      }
    }
  }

  test("DISTINCT and dropDuplicates on a collated column are collation-correct") {
    withHelloTable { tblName =>
      // These lower to aggregates after this rule runs, so they are not rewritten and keep using
      // sort-based aggregation; only their collation correctness is asserted here.
      checkAnswer(
        sql(s"SELECT DISTINCT c1 FROM $tblName").selectExpr("LOWER(c1)"),
        Seq(Row("hello")))
      checkAnswer(
        spark.table(tblName).dropDuplicates("c1").selectExpr("LOWER(c1)"),
        Seq(Row("hello")))
    }
  }

  test("set operations on collated columns are collation-correct") {
    val left = "set_op_left"
    val right = "set_op_right"
    withTable(left, right) {
      sql(s"CREATE TABLE $left (c1 STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"CREATE TABLE $right (c1 STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"INSERT INTO $left VALUES ('Apple'), ('APPLE'), ('banana')")
      sql(s"INSERT INTO $right VALUES ('apple')")

      // Distinct set operations lower to joins and the ALL variants lower to a grouping Aggregate
      // under a Generate; this rule does not rewrite those, so verify collation-correct results
      // (and, for the ALL variants, that they still plan and run).
      checkAnswer(
        sql(s"SELECT c1 FROM $left INTERSECT SELECT c1 FROM $right").selectExpr("LOWER(c1)"),
        Seq(Row("apple")))
      checkAnswer(
        sql(s"SELECT c1 FROM $left EXCEPT SELECT c1 FROM $right").selectExpr("LOWER(c1)"),
        Seq(Row("banana")))
      checkAnswer(
        sql(s"SELECT c1 FROM $left INTERSECT ALL SELECT c1 FROM $right").selectExpr("LOWER(c1)"),
        Seq(Row("apple")))
      checkAnswer(
        sql(s"SELECT c1 FROM $left EXCEPT ALL SELECT c1 FROM $right").selectExpr("LOWER(c1)"),
        Seq(Row("apple"), Row("banana")))
    }
  }

  test("OptimizeExpand does not add raw-collated pre-aggregate") {
    val tblName = "optimize_expand_collation"
    withSQLConf(SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
      withTable(tblName) {
        sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT, c3 INT) " +
          "USING PARQUET")
        sql(s"INSERT INTO $tblName VALUES ('A', 1, 10), ('a', 2, 10), ('A', 2, 20)")

        val df = sql(
          s"""SELECT COUNT(DISTINCT c2) AS d2, COUNT(DISTINCT c3) AS d3
             |FROM $tblName GROUP BY c1""".stripMargin)
        assert(!df.queryExecution.optimizedPlan.exists {
          case e: Expand => e.child.isInstanceOf[Aggregate]
          case _ => false
        })
        assert(usesHashBasedAggregate(df))
        assert(!usesSortAggregate(df))
        checkAnswer(df, Seq(Row(2, 2)))
      }
    }
  }

  test("streaming aggregate on a collated grouping key is not rewritten") {
    implicit val sqlCtx = spark.sqlContext
    import testImplicits._
    val input = MemoryStream[String]
    val df = input.toDF().selectExpr("value COLLATE UTF8_LCASE AS c").groupBy("c").count()

    // Sanity check: the analyzed plan is a streaming aggregate whose grouping key would otherwise
    // be normalizable, so it is the streaming guard (not a missing precondition) that must prevent
    // the rewrite from changing the state store key schema.
    val analyzed = df.queryExecution.analyzed
    assert(analyzed.exists { case a: Aggregate => a.isStreaming; case _ => false })

    val rewritten = RewriteCollationAggregate(analyzed)
    assert(rewritten.find(_.expressions.exists(_.exists(_.isInstanceOf[CollationKey]))).isEmpty)
  }

  test("falls back to sort aggregation when hash aggregation for collated keys is disabled") {
    withSQLConf(SQLConf.COLLATION_HASH_AGGREGATION_ENABLED.key -> "false") {
      withHelloTable { tblName =>
        val df = sql(s"SELECT COUNT(*) FROM $tblName GROUP BY c1")
        assert(usesSortAggregate(df))
        assert(!usesHashAggregate(df))
        assert(!usesObjectHashAggregate(df))
        checkAnswer(df, Seq(Row(3)))

        // Results stay collation-correct on the sort-based path.
        checkAnswer(
          sql(s"SELECT LOWER(c1) AS k, COUNT(*) AS cnt FROM $tblName GROUP BY c1"),
          Seq(Row("hello", 3)))
      }
    }
  }

  test("forcing object hash aggregate produces collation-correct results") {
    // Previously, forcing object hash aggregation on a collated grouping key produced incorrect
    // results because grouping was done on raw bytes. After normalizing the grouping key with its
    // collation key, forced object hash aggregation groups collation-equal rows together.
    withSQLConf("spark.sql.test.forceApplyObjectHashAggregate" -> "true") {
      withHelloTable { tblName =>
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tblName GROUP BY c1"),
          Seq(Row(3)))

        checkAnswer(
          sql(s"SELECT ARRAY_SORT(COLLECT_LIST(c2)) AS c3 FROM $tblName GROUP BY c1"),
          Seq(Row(Seq(1, 2, 3))))
      }
    }
  }
}
