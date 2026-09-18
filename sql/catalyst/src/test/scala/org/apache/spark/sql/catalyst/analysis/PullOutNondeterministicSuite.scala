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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, LogicalPlan, Sort}

/**
 * Test suite for moving non-deterministic expressions into Project.
 */
class PullOutNondeterministicSuite extends AnalysisTest {

  private lazy val a = $"a".int
  private lazy val b = $"b".int
  private lazy val r = LocalRelation(a, b)
  private lazy val rnd = Rand(10).as("_nondeterministic")
  private lazy val rndref = rnd.toAttribute

  private def analyze(sqlText: String): LogicalPlan = {
    getAnalyzer.executeAndCheck(parsePlan(sqlText), new QueryPlanningTracker)
  }

  test("no-op on filter") {
    checkAnalysis(
      r.where(Rand(10) > Literal(1.0)),
      r.where(Rand(10) > Literal(1.0))
    )
  }

  test("sort") {
    checkAnalysis(
      r.sortBy(SortOrder(Rand(10), Ascending)),
      r.select(a, b, rnd).sortBy(SortOrder(rndref, Ascending)).select(a, b)
    )
  }

  test("aggregate") {
    checkAnalysis(
      r.groupBy(Rand(10))(Rand(10).as("rnd")),
      r.select(a, b, rnd).groupBy(rndref)(rndref.as("rnd"))
    )
  }

  test("aes_encrypt with a foldable fixed IV is deterministic") {
    val analyzed = analyze(
      """SELECT aes_encrypt(
        |  'Spark',
        |  '0000111122223333',
        |  'GCM',
        |  'DEFAULT',
        |  unhex('000000000000000000000000'))
        |""".stripMargin)
    val aesEncrypt = analyzed.expressions.flatMap(_.collect {
      case aesEncrypt: AesEncrypt => aesEncrypt
    }).head

    assert(aesEncrypt.deterministic)
    assert(aesEncrypt.replacement.deterministic)
    assert(aesEncrypt.replacement.foldable)
  }

  test("pull out aes_encrypt that generates a random IV") {
    val queries = Seq(
      """SELECT aes_encrypt('Spark', '0000111122223333') AS encrypted
        |FROM TaBlE
        |GROUP BY aes_encrypt('Spark', '0000111122223333')
        |""".stripMargin,
      """SELECT * FROM TaBlE
        |ORDER BY aes_encrypt('Spark', '0000111122223333')
        |""".stripMargin)

    queries.foreach { sqlText =>
      val analyzed = analyze(sqlText)

      val nondeterministicOperatorExpressions = analyzed.collect {
        case aggregate: Aggregate => aggregate.groupingExpressions
        case sort: Sort => sort.order
      }.flatten.filterNot(_.deterministic)
      assert(nondeterministicOperatorExpressions.isEmpty)

      val aesEncryptCount = analyzed.collect {
        case plan => plan.expressions.flatMap(_.collect { case _: AesEncrypt => 1 }).sum
      }.sum
      assert(aesEncryptCount == 1)
    }
  }
}
