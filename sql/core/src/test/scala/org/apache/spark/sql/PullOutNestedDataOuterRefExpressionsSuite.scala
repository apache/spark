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

package org.apache.spark.sql

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, GetStructField, OuterReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.optimizer.PullOutNestedDataOuterRefExpressions
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

/**
 * Tests for [[PullOutNestedDataOuterRefExpressions]], the rule that projects a nested-data member
 * of an outer column (`outer_map[1]`, `c.home.zip`, `variant_get(c.home_v, '$.zip')`) onto the
 * outer plan and rewrites the correlation to a bare `OuterReference` on that projection, so that
 * decorrelation keys on the member rather than on the whole outer column.
 *
 * The analyzer resolves `c.home.zip` inside-out as `GetStructField(OuterReference(home), zip)`,
 * not as an `OuterReference`. Without the rule the whole `home` struct is the correlated key: a
 * correlated equality on the member goes through a domain join on the whole struct, and a
 * pulled-up join condition on the member is left unresolved (`INTERNAL_ERROR`). The rule-off tests
 * below use [[SQLConf.PULL_OUT_NESTED_DATA_OUTER_REF_EXPRESSIONS_ENABLED]] to reproduce the legacy
 * behavior.
 */
class PullOutNestedDataOuterRefExpressionsSuite extends QueryTest with SharedSparkSession {

  private val ruleFlag = SQLConf.PULL_OUT_NESTED_DATA_OUTER_REF_EXPRESSIONS_ENABLED.key

  private val oneRowPerCustomerMax = Seq(Row(1, 70), Row(2, 70), Row(3, 20), Row(4, null))

  /**
   * Customers 1 and 2 share `home.zip = 94105` but differ in `region`, so keying on the whole
   * `home` struct is observably different from keying on the `home.zip` field. Orders 50 and 70
   * share `ship.zip = 94105` but differ in `city` for the same reason on the inner side. `pzip` is
   * a plain column equal to `ship.zip`, for correlations whose inner side is not a struct field.
   */
  private def withOuterStructTables(testBody: => Unit): Unit = {
    withTable("osf_c", "osf_o") {
      sql("CREATE TABLE osf_c (id INT, home STRUCT<zip: INT, region: STRING>, home_v VARIANT) " +
        "USING parquet")
      sql("INSERT INTO osf_c VALUES " +
        "(1, named_struct('zip', 94105, 'region', 'west'), parse_json('{\"zip\": 94105}')), " +
        "(2, named_struct('zip', 94105, 'region', 'east'), parse_json('{\"zip\": 94105}')), " +
        "(3, named_struct('zip', 10001, 'region', 'ne'), parse_json('{\"zip\": 10001}')), " +
        "(4, NULL, NULL)")
      sql("CREATE TABLE osf_o (amount INT, ship STRUCT<zip: INT, city: STRING>, pzip INT) " +
        "USING parquet")
      sql("INSERT INTO osf_o VALUES " +
        "(50, named_struct('zip', 94105, 'city', 'SF'), 94105), " +
        "(70, named_struct('zip', 94105, 'city', 'San Francisco'), 94105), " +
        "(20, named_struct('zip', 10001, 'city', 'NYC'), 10001), " +
        "(5, named_struct('zip', CAST(NULL AS INT), 'city', 'A'), CAST(NULL AS INT)), " +
        "(6, named_struct('zip', CAST(NULL AS INT), 'city', 'B'), CAST(NULL AS INT))")
      testBody
    }
  }

  /** Whether any subquery plan still accesses a struct field of an outer reference. */
  private def containsOuterStructFieldAccess(plan: LogicalPlan): Boolean = {
    plan.exists(_.expressions.exists(_.exists {
      case g: GetStructField => g.child.isInstanceOf[OuterReference]
      case s: SubqueryExpression => containsOuterStructFieldAccess(s.plan)
      case _ => false
    }))
  }

  private def joins(df: DataFrame): Seq[Join] =
    df.queryExecution.optimizedPlan.collect { case j: Join => j }

  // The inner side is a plain column; the result is one row per outer row with and without the
  // rule, and with the rule no join keys on the whole struct.
  test("plain inner column equal to an outer struct field in a filter, rule on and off") {
    withOuterStructTables {
      val query =
        "SELECT c.id, (SELECT max(o.amount) FROM osf_o o WHERE o.pzip = c.home.zip) FROM osf_c c"
      Seq("true", "false").foreach { enabled =>
        withSQLConf(ruleFlag -> enabled) {
          val df = sql(query)
          checkAnswer(df, oneRowPerCustomerMax)
          if (enabled == "true") {
            assert(!joins(df).exists(_.condition.exists(_.exists {
              case a: Attribute => a.dataType.isInstanceOf[StructType]
              case _ => false
            })), df.queryExecution.optimizedPlan)
          }
        }
      }
    }
  }

  // In a JOIN ON the correlation is pulled up as a join condition. With the rule it references
  // the projected member; without it the outer struct field is left in the join condition and
  // decorrelation fails with an internal error.
  test("plain inner column equal to an outer struct field in a join condition, rule on and off") {
    withOuterStructTables {
      val query = "SELECT c.id, (SELECT max(o.amount) FROM osf_o o JOIN (SELECT 1 AS one) t " +
        "ON o.pzip = c.home.zip) FROM osf_c c"
      withSQLConf(ruleFlag -> "true", SQLConf.DECORRELATE_JOIN_PREDICATE_ENABLED.key -> "true") {
        checkAnswer(sql(query), oneRowPerCustomerMax)
      }
      withSQLConf(ruleFlag -> "false", SQLConf.DECORRELATE_JOIN_PREDICATE_ENABLED.key -> "true") {
        val e = intercept[SparkException](sql(query).collect())
        assert(e.getCondition == "INTERNAL_ERROR", e)
      }
    }
  }

  // The rule is non-excludable: asking the optimizer to exclude it must not reinstate the
  // whole-struct keying, so the join-condition form that fails without the rule still succeeds.
  test("the rule cannot be disabled through spark.sql.optimizer.excludedRules") {
    withOuterStructTables {
      withSQLConf(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> PullOutNestedDataOuterRefExpressions.ruleName,
        SQLConf.DECORRELATE_JOIN_PREDICATE_ENABLED.key -> "true") {
        checkAnswer(
          sql("SELECT c.id, (SELECT max(o.amount) FROM osf_o o JOIN (SELECT 1 AS one) t " +
            "ON o.pzip = c.home.zip) FROM osf_c c"),
          oneRowPerCustomerMax)
      }
    }
  }

  test("inner struct/variant field equal to an outer struct/variant field returns one row per " +
    "outer row") {
    withOuterStructTables {
      withSQLConf(ruleFlag -> "true") {
        checkAnswer(
          sql("SELECT c.id, (SELECT max(o.amount) FROM osf_o o WHERE o.ship.zip = c.home.zip) " +
            "FROM osf_c c"),
          oneRowPerCustomerMax)
        checkAnswer(
          sql("SELECT c.id, (SELECT count(*) FROM osf_o o WHERE o.ship.zip = c.home.zip) " +
            "FROM osf_c c"),
          Seq(Row(1, 2L), Row(2, 2L), Row(3, 1L), Row(4, 0L)))
        // Null-safe equality with ORDER BY ... LIMIT: NULL home.zip matches the NULL-zip orders.
        checkAnswer(
          sql("SELECT c.id, (SELECT o.amount FROM osf_o o WHERE o.ship.zip <=> c.home.zip " +
            "ORDER BY o.amount DESC LIMIT 1) FROM osf_c c"),
          Seq(Row(1, 70), Row(2, 70), Row(3, 20), Row(4, 6)))
        // Outer side is a variant field (VariantGet(OuterReference)); inner side a struct field.
        checkAnswer(
          sql("SELECT c.id, (SELECT max(o.amount) FROM osf_o o " +
            "WHERE o.ship.zip = variant_get(c.home_v, '$.zip', 'int')) FROM osf_c c"),
          oneRowPerCustomerMax)
      }
    }
  }

  // An inequality cannot be pulled up over the aggregate and goes through domain-join generation.
  // Keying the domain on the pulled-out field (or, with the rule off, on the whole struct, which is
  // a finer partition) only ever splits outer rows, never merges them, so the join-back yields one
  // row per outer row either way.
  test("inequality correlation on an outer struct field stays correct through the domain join") {
    withOuterStructTables {
      Seq("true", "false").foreach { enabled =>
        withSQLConf(ruleFlag -> enabled) {
          checkAnswer(
            sql("SELECT c.id, (SELECT max(o.amount) FROM osf_o o WHERE o.ship.zip < c.home.zip) " +
              "FROM osf_c c"),
            Seq(Row(1, 20), Row(2, 20), Row(3, null), Row(4, null)))
        }
      }
    }
  }

  test("inner struct field equal to an outer struct field in a join condition over an aggregate") {
    withOuterStructTables {
      withSQLConf(ruleFlag -> "true", SQLConf.DECORRELATE_JOIN_PREDICATE_ENABLED.key -> "true") {
        checkAnswer(
          sql("SELECT c.id, (SELECT max(o.amount) FROM osf_o o JOIN (SELECT 1 AS one) t " +
            "ON o.ship.zip = c.home.zip) FROM osf_c c"),
          oneRowPerCustomerMax)
      }
    }
  }

  // Plan-level check: the member is projected exactly once onto the outer plan, no struct access
  // over an outer reference remains, and the subquery's outer attribute is the projected member.
  test("the member is projected once and the subquery's outer attribute is rewritten") {
    withOuterStructTables {
      withSQLConf(ruleFlag -> "true") {
        val analyzed = sql("SELECT c.id FROM osf_c c WHERE EXISTS (SELECT 1 FROM osf_o o " +
          "WHERE o.pzip = c.home.zip AND o.amount > c.home.zip - 94100)")
          .queryExecution.analyzed
        assert(containsOuterStructFieldAccess(analyzed))

        val rewritten = PullOutNestedDataOuterRefExpressions(analyzed)
        assert(!containsOuterStructFieldAccess(rewritten), rewritten)

        val members = rewritten.collect { case p: Project =>
          p.projectList.collect { case a: Alias if a.child.isInstanceOf[GetStructField] => a }
        }.flatten
        assert(members.size == 1, rewritten)
        val member = members.head.toAttribute

        val Seq(exists) = rewritten.flatMap(_.expressions.flatMap(_.collect {
          case s: SubqueryExpression => s
        }))
        assert(exists.getOuterAttrs.map { case a: Attribute => a.exprId } == Seq(member.exprId),
          exists)
      }
    }
  }
}
