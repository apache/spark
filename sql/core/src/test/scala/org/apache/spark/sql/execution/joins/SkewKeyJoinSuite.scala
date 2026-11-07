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

package org.apache.spark.sql.execution.joins

import org.apache.logging.log4j.Level

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, Join, JoinHint, SKEW_JOIN}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SkewKeyJoinSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  // The key 7 is hot on both sides, the key 9 is less frequent, and every tenth row has a null key.
  // The second key has three values.
  private def createTables(): Unit = {
    sql(
      """SELECT CASE WHEN id % 10 = 0 THEN NULL WHEN id % 2 = 0 THEN 7 WHEN id % 10 = 5 THEN 9
          |    ELSE id END AS k1,
        |  id % 3 AS k2, id AS v
        |FROM range(1000)""".stripMargin).createOrReplaceTempView("t1")
    sql(
      """SELECT CASE WHEN id % 10 = 0 THEN NULL WHEN id % 2 = 0 THEN 7 WHEN id % 10 = 5 THEN 9
          |    ELSE id END AS k1,
        |  id % 3 AS k2, id AS w
        |FROM range(300)""".stripMargin).createOrReplaceTempView("t2")
    sql("SELECT CAST(k1 AS INT) AS i, w FROM t2").createOrReplaceTempView("t3")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTables()
  }

  private def withSkewJoinConfs(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "4",
      SQLConf.SKEW_JOIN_HINT_SAMPLE_FRACTION.key -> "1.0",
      SQLConf.SKEW_JOIN_HINT_MIN_HOT_KEY_ROWS.key -> "10",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1")(f)
  }

  private def skewKeyJoins(df: DataFrame): Seq[SkewKeyJoinExec] = {
    collect(df.queryExecution.executedPlan) { case j: SkewKeyJoinExec => j }
  }

  /**
   * Checks that the query with the hint is planned as a [[SkewKeyJoinExec]] that finds hot keys,
   * and returns the same rows as the query without the hint.
   */
  private def checkSkewKeyJoin(hint: String, joinClause: String, hasHotKeys: Boolean = true)
    : Unit = {
    val hinted = sql(s"SELECT /*+ $hint */ * FROM $joinClause")
    val expected = sql(s"SELECT * FROM $joinClause")
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      checkAnswer(hinted, expected)
      val joins = skewKeyJoins(hinted)
      assert(joins.size == 1)
      assert((joins.head.metrics("numHotKeys").value > 0) == hasHotKeys)
    }
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      checkAnswer(sql(s"SELECT /*+ $hint */ * FROM $joinClause"), expected)
    }
  }

  test("SKEW_JOIN hint is resolved on the hinted relation") {
    val df = sql("SELECT /*+ SKEW_JOIN(t1(k1, k2)) */ * FROM t1 JOIN t2 ON t1.k1 = t2.k1")
    val hints = df.queryExecution.optimizedPlan.collect { case j: Join => j.hint }
    assert(hints == Seq(
      JoinHint(Some(HintInfo(Some(SKEW_JOIN), Seq("k1", "k2"))), None)))

    val df2 = sql("SELECT /*+ SKEWJOIN(t2(k1)) */ * FROM t1 JOIN t2 ON t1.k1 = t2.k1")
    val hints2 = df2.queryExecution.optimizedPlan.collect { case j: Join => j.hint }
    assert(hints2 == Seq(JoinHint(None, Some(HintInfo(Some(SKEW_JOIN), Seq("k1"))))))
  }

  test("inner join with a hint on the left side") {
    withSkewJoinConfs {
      checkSkewKeyJoin("SKEW_JOIN(t1(k1))", "t1 JOIN t2 ON t1.k1 = t2.k1")
    }
  }

  test("inner join with a hint on the right side") {
    withSkewJoinConfs {
      checkSkewKeyJoin("SKEW_JOIN(t2(k1))", "t1 JOIN t2 ON t1.k1 = t2.k1")
    }
  }

  test("inner join on multiple keys") {
    withSkewJoinConfs {
      // The rows of the key 7 are split over the values of the second key, so use more
      // partitions for the pairs to be hot.
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "100") {
        checkSkewKeyJoin("SKEW_JOIN(t1(k1, k2))",
          "t1 JOIN t2 ON t1.k1 = t2.k1 AND t1.k2 = t2.k2")
      }
    }
  }

  test("hint on a subset of the join keys") {
    withSkewJoinConfs {
      checkSkewKeyJoin("SKEW_JOIN(t1(k1))", "t1 JOIN t2 ON t1.k1 = t2.k1 AND t1.k2 = t2.k2")
    }
  }

  test("inner join with a non equi-join condition") {
    withSkewJoinConfs {
      checkSkewKeyJoin("SKEW_JOIN(t1(k1))", "t1 JOIN t2 ON t1.k1 = t2.k1 AND t1.v > t2.w")
    }
  }

  test("left outer join with a hint on the left side") {
    withSkewJoinConfs {
      checkSkewKeyJoin("SKEW_JOIN(t1(k1))", "t1 LEFT JOIN t2 ON t1.k1 = t2.k1")
      checkSkewKeyJoin("SKEW_JOIN(t1(k1))",
        "t1 LEFT JOIN t2 ON t1.k1 = t2.k1 AND t1.v > t2.w")
    }
  }

  test("right outer join with a hint on the right side") {
    withSkewJoinConfs {
      checkSkewKeyJoin("SKEW_JOIN(t2(k1))", "t1 RIGHT JOIN t2 ON t1.k1 = t2.k1")
      checkSkewKeyJoin("SKEW_JOIN(t2(k1))",
        "t1 RIGHT JOIN t2 ON t1.k1 = t2.k1 AND t1.v > t2.w")
    }
  }

  test("null keys are not hot and keep their outer join semantics") {
    withSkewJoinConfs {
      val query = "SELECT /*+ SKEW_JOIN(t1(k1)) */ t1.v, t2.w FROM t1 LEFT JOIN t2 " +
        "ON t1.k1 = t2.k1 WHERE t1.k1 IS NULL"
      val df = sql(query)
      checkAnswer(df, sql("SELECT t1.v, CAST(NULL AS BIGINT) FROM t1 WHERE t1.k1 IS NULL"))
    }
  }

  test("no join is split if no key is hot") {
    withSkewJoinConfs {
      withSQLConf(SQLConf.SKEW_JOIN_HINT_MIN_HOT_KEY_ROWS.key -> "100000") {
        checkSkewKeyJoin("SKEW_JOIN(t1(k1))", "t1 JOIN t2 ON t1.k1 = t2.k1", hasHotKeys = false)
      }
    }
  }

  test("at most the configured number of hot keys is used") {
    withSkewJoinConfs {
      // With 100 shuffle partitions, both the key 7 and the key 9 are hot.
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "100") {
        Seq("2" -> 2L, "1" -> 1L).foreach { case (maxHotKeys, expectedHotKeys) =>
          withSQLConf(SQLConf.SKEW_JOIN_HINT_MAX_HOT_KEYS.key -> maxHotKeys) {
            val df = sql("SELECT /*+ SKEW_JOIN(t1(k1)) */ * FROM t1 JOIN t2 ON t1.k1 = t2.k1")
            checkAnswer(df, sql("SELECT * FROM t1 JOIN t2 ON t1.k1 = t2.k1"))
            assert(skewKeyJoins(df).head.metrics("numHotKeys").value == expectedHotKeys)
          }
        }
      }
    }
  }

  test("the hot keys are found by sampling the skewed side") {
    withSkewJoinConfs {
      withSQLConf(SQLConf.SKEW_JOIN_HINT_SAMPLE_FRACTION.key -> "0.5") {
        checkSkewKeyJoin("SKEW_JOIN(t1(k1))", "t1 JOIN t2 ON t1.k1 = t2.k1")
      }
    }
  }

  test("hint is ignored with a warning if the join cannot be split") {
    withSkewJoinConfs {
      // (hint, join, reason)
      Seq(
        ("SKEW_JOIN(t1(k1))", "t1 FULL JOIN t2 ON t1.k1 = t2.k1", "full outer join"),
        // The left outer join broadcasts the right side, so only the left side can be split.
        ("SKEW_JOIN(t2(k1))", "t1 LEFT JOIN t2 ON t1.k1 = t2.k1", "left outer join"),
        ("SKEW_JOIN(t1(v))", "t1 JOIN t2 ON t1.k1 = t2.k1", "not all equi-join keys"),
        // The int key of t3 is cast to the type of the key of t1.
        ("SKEW_JOIN(t1(k1))", "t1 JOIN t3 ON t1.k1 = t3.i", "types of the join keys")
      ).foreach { case (hint, join, reason) =>
        val appender = new LogAppender("skew join hint")
        withLogAppender(appender) {
          val hinted = sql(s"SELECT /*+ $hint */ * FROM $join")
          checkAnswer(hinted, sql(s"SELECT * FROM $join"))
          assert(skewKeyJoins(hinted).isEmpty, s"$hint on $join")
        }
        val warnings = appender.loggingEvents.filter(_.getLevel == Level.WARN)
          .map(_.getMessage.getFormattedMessage)
        assert(warnings.exists(m => m.contains("skew_join") && m.contains(reason)),
          s"$hint on $join: $warnings")
      }
    }
  }

  test("hint is ignored for key types whose values cannot be compared by value") {
    withSkewJoinConfs {
      sql("SELECT CAST(k1 AS DOUBLE) AS d FROM t1").createOrReplaceTempView("d1")
      sql("SELECT CAST(k1 AS DOUBLE) AS d FROM t2").createOrReplaceTempView("d2")
      val hinted = sql("SELECT /*+ SKEW_JOIN(d1(d)) */ * FROM d1 JOIN d2 ON d1.d = d2.d")
      checkAnswer(hinted, sql("SELECT * FROM d1 JOIN d2 ON d1.d = d2.d"))
      assert(skewKeyJoins(hinted).isEmpty)
    }
  }

  test("inner join with a hint on both sides splits the left side") {
    withSkewJoinConfs {
      val df = sql("SELECT /*+ SKEW_JOIN(t1(k1), t2(k1)) */ * FROM t1 JOIN t2 ON t1.k1 = t2.k1")
      checkAnswer(df, sql("SELECT * FROM t1 JOIN t2 ON t1.k1 = t2.k1"))
      assert(skewKeyJoins(df).map(_.skewLeft) == Seq(true))
    }
  }

  test("hint on a join with non-deterministic expressions is rejected") {
    withSkewJoinConfs {
      val e = intercept[AnalysisException] {
        sql("""SELECT /*+ SKEW_JOIN(t1(k1)) */ *
          |FROM t1 JOIN (SELECT k1, rand() AS r FROM t2) t2 ON t1.k1 = t2.k1""".stripMargin)
          .collect()
      }
      checkError(
        exception = e,
        condition = "INVALID_SKEW_JOIN_HINT",
        parameters = Map("reason" ->
          ("the join has non-deterministic expressions, but both sides of the join are " +
            "read twice")))
    }
  }

  test("hint on a column that is not in the relation is rejected") {
    withSkewJoinConfs {
      val e = intercept[AnalysisException] {
        sql("SELECT /*+ SKEW_JOIN(t1(nope)) */ * FROM t1 JOIN t2 ON t1.k1 = t2.k1").collect()
      }
      checkError(
        exception = e,
        condition = "INVALID_SKEW_JOIN_HINT",
        parameters = Map("reason" -> "the column nope is not found in the relation"))
    }
  }

  test("hint without columns is rejected") {
    Seq("SKEW_JOIN", "SKEW_JOIN(t1)", "SKEW_JOIN(t1(1))").foreach { hint =>
      val e = intercept[AnalysisException] {
        sql(s"SELECT /*+ $hint */ * FROM t1 JOIN t2 ON t1.k1 = t2.k1").collect()
      }
      assert(e.getCondition == "INVALID_SKEW_JOIN_HINT", hint)
    }
  }
}
