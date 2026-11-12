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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.{parseExpression, parsePlan}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, LogicalPlan, Project, RepartitionByExpression, Sort}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

/**
 * Test suite for moving non-deterministic expressions into Project.
 */
class PullOutNondeterministicSuite extends AnalysisTest {

  private lazy val a = $"a".int
  private lazy val b = $"b".int
  private lazy val r = LocalRelation(a, b)
  private lazy val rnd = Rand(10).as("_nondeterministic")
  private lazy val rndref = rnd.toAttribute

  /** Resolve a SQL query through the complete analyzer, including nondeterministic extraction. */
  private def analyze(sqlText: String): LogicalPlan = {
    getAnalyzer.executeAndCheck(parsePlan(sqlText), new QueryPlanningTracker)
  }

  /** Build repartitioning directly because CatalystSqlParser does not support DISTRIBUTE BY. */
  private def analyzeClause(input: String, clause: String, expression: String): LogicalPlan = {
    val plan = if (clause == "DISTRIBUTE BY") {
      RepartitionByExpression(Seq(parseExpression(expression)), parsePlan(input), None)
    } else {
      parsePlan(s"$input $clause $expression")
    }
    getAnalyzer.executeAndCheck(plan, new QueryPlanningTracker)
  }

  /** Verify that every operator shares one AES evaluation in its original evaluation context. */
  private def checkAesExtraction(
      expression: String,
      isEvaluationUnit: Expression => Boolean,
      clauses: Seq[String] = Seq("GROUP BY", "ORDER BY", "DISTRIBUTE BY")): Unit = {
    clauses.foreach { clause =>
      val select = if (clause == "GROUP BY") "count(*)" else "*"
      val analyzed = analyzeClause(
        s"""SELECT $select FROM VALUES ('Spark', '0000111122223333'), ('SQL', 'short')
           |AS t(v, k)
           |""".stripMargin, clause, expression)
      val operatorExpressions = analyzed.collect {
        case a: Aggregate => a.groupingExpressions
        case s: Sort => s.order
        case r: RepartitionByExpression => r.partitionExpressions
      }.flatten
      assert(operatorExpressions.forall(_.deterministic), analyzed.treeString)
      assert(analyzed.collect { case p if p.missingInput.nonEmpty => p }.isEmpty)

      val extracted = analyzed.collect {
        case p: Project => p.projectList.collect {
          case a: Alias if a.child.exists(_.isInstanceOf[AesEncrypt]) => a.child
        }
      }.flatten
      assert(extracted.size == 1, analyzed.treeString)
      assert(isEvaluationUnit(extracted.head), analyzed.treeString)
      val aesCount = analyzed.collect {
        case p => p.expressions.flatMap(_.collect { case _: AesEncrypt => 1 }).sum
      }.sum
      assert(aesCount == 1, analyzed.treeString)
    }
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

  // Analysis must recognize fixed IVs before optimizer constant folding runs.
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

  // SQL wrappers must retain the determinism of their constant replacement values.
  test("fixed AES arguments include runtime-replaceable expressions") {
    for {
      (mode, length) <- Seq("GCM" -> 12, "CBC" -> 16)
      iv <- Seq(
        s"to_binary('${"00" * length}', 'hex')",
        s"encode('${"0" * length}', 'utf-8')")
    } {
      val expression = s"aes_encrypt('Spark', '0000111122223333', '$mode', 'DEFAULT', $iv)"
      val analyzed = analyze(s"SELECT $expression")
      val aes = analyzed.expressions.flatMap(_.collect { case a: AesEncrypt => a }).head
      assert(aes.deterministic)
      val replacement = ReplaceExpressions.replace(aes)
      assert(replacement.deterministic)
      assert(replacement.foldable)
      analyze(s"SELECT * FROM TaBlE t JOIN TaBlE2 s ON $expression = X'01'")
    }

    val analyzed = analyze("SELECT aes_encrypt('Spark', '0000111122223333', lower('ECB'))")
    val aes = analyzed.expressions.flatMap(_.collect { case a: AesEncrypt => a }).head
    assert(aes.deterministic)
    assert(aes.replacement.foldable)
  }

  // Extract the guard together with AES so skipped branches remain unevaluated.
  test("AES extraction retains conditional evaluation") {
    Seq(
      "CASE WHEN octet_length(k) = 16 THEN aes_encrypt(v, k) END",
      "if(octet_length(k) = 16, aes_encrypt(v, k), NULL)",
      "coalesce(if(octet_length(k) <> 16, X'01', NULL), aes_encrypt(v, k))"
    ).foreach { expression =>
      checkAesExtraction(expression, _.isInstanceOf[ConditionalExpression])
    }
    checkAesExtraction(
      "octet_length(k) = 16 AND aes_encrypt(v, k) IS NOT NULL", _.isInstanceOf[And])
    checkAesExtraction(
      "octet_length(k) <> 16 OR aes_encrypt(v, k) IS NOT NULL", _.isInstanceOf[Or])
  }

  // Extracting the higher-order function keeps AES inputs bound to their lambda.
  test("AES extraction retains lambda scope") {
    checkAesExtraction(
      "transform(array(v), x -> aes_encrypt(x, '0000111122223333'))",
      _.isInstanceOf[ArrayTransform])
    checkAesExtraction(
      "size(transform(array(v), x -> aes_encrypt(x, '0000111122223333')))",
      _.isInstanceOf[ArrayTransform])
  }

  // TryEval must catch errors from AES after extraction, as it does inside a projection.
  test("AES extraction retains try evaluation") {
    Seq(
      "try_aes_decrypt(aes_encrypt(v, k), k)",
      "try_to_binary(hex(aes_encrypt(v, k)), 'hex')"
    ).foreach { expression =>
      checkAesExtraction(expression, _.isInstanceOf[TryEval])
    }
  }

  // Aggregate can move valid predicate subqueries together with their AES evaluation context.
  test("AES grouping extraction retains guards containing predicate subqueries") {
    val predicate = "k IN (SELECT a FROM VALUES ('0000111122223333') AS s(a))"
    Seq(
      s"CASE WHEN $predicate THEN aes_encrypt(v, k) END",
      s"if($predicate, aes_encrypt(v, k), NULL)",
      s"coalesce(if(NOT ($predicate), X'01', NULL), aes_encrypt(v, k))",
      s"$predicate AND aes_encrypt(v, k) IS NOT NULL",
      s"NOT ($predicate) OR aes_encrypt(v, k) IS NOT NULL",
      s"aes_encrypt(CAST($predicate AS STRING), '0000111122223333')",
      "aes_encrypt(CAST(EXISTS (SELECT 1) AS STRING), '0000111122223333')"
    ).foreach { expression =>
      checkAesExtraction(expression, _ => true, Seq("GROUP BY"))
    }
    withSQLConf(
      SQLConf.ALLOW_SUBQUERY_EXPRESSIONS_IN_LAMBDAS_AND_HIGHER_ORDER_FUNCTIONS.key -> "true") {
      checkAesExtraction(
        s"transform(array(CAST($predicate AS STRING)), x -> aes_encrypt(x, k))",
        _.isInstanceOf[ArrayTransform], Seq("GROUP BY"))
    }
  }

  // Nondeterministic UDF wrappers over deterministic predicates keep their original grouping plan.
  test("nondeterministic UDF grouping extraction accepts predicate subqueries") {
    Seq("a IN (SELECT a FROM TaBlE2)", "EXISTS (SELECT a FROM TaBlE2)").foreach { predicate =>
      val projection = analyze(s"SELECT $predicate AS p FROM TaBlE").asInstanceOf[Project]
      val udf = ScalaUDF((value: Boolean) => value, BooleanType,
        Seq(projection.projectList.head.asInstanceOf[Alias].child), udfDeterministic = false)
      val aggregate = Aggregate(Seq(udf), Seq(Literal(1).as("n")),
        projection.child)
      val analyzed = getAnalyzer.executeAndCheck(aggregate, new QueryPlanningTracker)
      assert(analyzed.collect { case a: Aggregate => a.groupingExpressions }
        .flatten.forall(_.deterministic), analyzed.treeString)
      val extracted = analyzed.collect {
        case p: Project => p.projectList.flatMap(_.collect { case u: ScalaUDF => u })
      }.flatten
      assert(extracted.size == 1, analyzed.treeString)
    }
  }

  // Predicate subqueries must reach placement validation in their original operator.
  test("predicate subqueries are not nondeterministic evaluation units") {
    Seq("GROUP BY", "ORDER BY", "DISTRIBUTE BY").foreach { clause =>
      Seq(
        "a IN (SELECT a FROM TaBlE WHERE rand(0) > 0.5)",
        "EXISTS (SELECT a FROM TaBlE WHERE rand(0) > 0.5)"
      ).foreach { expression =>
        val select = if (clause == "GROUP BY") "count(*)" else "*"
        val query = s"SELECT $select FROM TaBlE"
        if (clause == "GROUP BY") {
          val error = intercept[SparkException](analyzeClause(query, clause, expression))
          assert(error.getCondition == "INTERNAL_ERROR")
        } else {
          val error = intercept[AnalysisException](analyzeClause(query, clause, expression))
          assert(error.getCondition ==
            "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_IN_EXISTS_SUBQUERY")
        }
      }
    }
  }

  // Unary operators must still reject predicate subqueries even inside an extracted AES wrapper.
  test("AES wrappers do not bypass predicate subquery placement validation") {
    for {
      clause <- Seq("ORDER BY", "DISTRIBUTE BY")
      predicate <- Seq("a IN (SELECT a FROM TaBlE2)", "EXISTS (SELECT a FROM TaBlE2)")
    } {
      val expression = s"aes_encrypt(CAST($predicate AS STRING), '0000111122223333')"
      val error = intercept[AnalysisException] {
        analyzeClause("SELECT * FROM TaBlE", clause, expression)
      }
      assert(error.getCondition ==
        "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_IN_EXISTS_SUBQUERY")
    }
  }

  // IN values can be extracted independently of the typed ListQuery child.
  test("nondeterministic IN values are extracted without extracting the list query") {
    val analyzed = analyze(
      "SELECT count(*) FROM TaBlE GROUP BY rand(0) IN (SELECT a FROM TaBlE)")
    val extracted = analyzed.collect {
      case p: Project => p.projectList.flatMap(_.collect { case r: Rand => r })
    }.flatten
    assert(extracted.size == 1, analyzed.treeString)
  }

  // Scalar subqueries retain the existing single-evaluation extraction behavior.
  test("scalar subqueries remain single nondeterministic evaluation units") {
    for {
      clause <- Seq("GROUP BY", "ORDER BY")
      subquery <- Seq(
        "SELECT rand(0)",
        "SELECT rand(0) WHERE 1 IN (SELECT a FROM TaBlE)",
        "SELECT rand(0) WHERE EXISTS (SELECT a FROM TaBlE)")
    } {
      val select = if (clause == "GROUP BY") "count(*)" else "*"
      val analyzed = analyze(s"SELECT $select FROM TaBlE $clause ($subquery)")
      val extracted = analyzed.collect {
        case p: Project => p.projectList.flatMap(_.collect { case s: ScalarSubquery => s })
      }.flatten
      assert(extracted.size == 1, analyzed.treeString)
    }
  }

  // Preserve the existing placement of rand when no new evaluation unit is present.
  test("legacy nondeterministic leaves retain conditional extraction") {
    checkAnalysis(
      r.sortBy(SortOrder(If(a > 0, Rand(10), Literal(0.0)), Ascending)),
      r.select(a, b, rnd)
        .sortBy(SortOrder(If(a > 0, rndref, Literal(0.0)), Ascending)).select(a, b))
  }

  // AND/OR and TryEval retain the established extraction of legacy leaves outside the wrapper.
  test("legacy nondeterministic leaves retain boolean and try extraction") {
    Seq[Expression](
      And(a > 0, Rand(10) > Literal(0.5)),
      Or(a > 0, Rand(10) > Literal(0.5)),
      TryEval(Rand(10))
    ).foreach { expression =>
      val expected = expression.transform { case _: Rand => rndref }
      checkAnalysis(
        r.sortBy(SortOrder(expression, Ascending)),
        r.select(a, b, rnd).sortBy(SortOrder(expected, Ascending)).select(a, b))
    }
    val udf = ScalaUDF((value: Int) => value, a.dataType, Seq(a), udfDeterministic = false)
    val extracted = Alias(udf, "_nondeterministic")()
    checkAnalysis(
      r.sortBy(SortOrder(TryEval(udf), Ascending)),
      r.select(a, b, extracted)
        .sortBy(SortOrder(TryEval(extracted.toAttribute), Ascending)).select(a, b))
  }

  // Legacy leaves in lambdas must move with the higher-order function to keep variables bound.
  test("legacy nondeterministic leaves retain lambda scope") {
    for {
      clause <- Seq("GROUP BY", "ORDER BY", "DISTRIBUTE BY")
      expression <- Seq(
        "transform(array(array(a)), x -> shuffle(x))",
        "transform(array(a), x -> x + rand(0))")
    } {
      val select = if (clause == "GROUP BY") "count(*)" else "*"
      val analyzed = analyzeClause(s"SELECT $select FROM TaBlE", clause, expression)
      assert(analyzed.collect { case p if p.missingInput.nonEmpty => p }.isEmpty)
      val extracted = analyzed.collect {
        case p: Project => p.projectList.collect {
          case a: Alias if a.child.isInstanceOf[ArrayTransform] => a.child
        }
      }.flatten
      assert(extracted.size == 1, analyzed.treeString)
      assert(analyzed.collect {
        case a: Aggregate => a.groupingExpressions
        case s: Sort => s.order
        case r: RepartitionByExpression => r.partitionExpressions
      }.flatten.forall(_.deterministic), analyzed.treeString)
    }
  }

  // Random-IV AES must run once below operators that require deterministic expressions.
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

  // Selecting enclosing AES must avoid duplicating its nested UUID evaluation.
  test("pull out random-IV aes_encrypt with a nondeterministic child") {
    val queries = Seq(
      """SELECT count(*)
        |FROM TaBlE
        |GROUP BY aes_encrypt(uuid(), '0000111122223333')
        |""".stripMargin,
      """SELECT * FROM TaBlE
        |ORDER BY aes_encrypt(uuid(), '0000111122223333')
        |""".stripMargin)

    queries.foreach { sqlText =>
      val analyzed = analyze(sqlText)

      val nondeterministicOperatorExpressions = analyzed.collect {
        case aggregate: Aggregate => aggregate.groupingExpressions
        case sort: Sort => sort.order
      }.flatten.filterNot(_.deterministic)
      assert(nondeterministicOperatorExpressions.isEmpty)

      val extractedExpressions = analyzed.collect {
        case plan => plan.expressions.flatMap(_.collect {
          case _: AesEncrypt => "aes_encrypt"
          case _: Uuid => "uuid"
        })
      }.flatten
      assert(extractedExpressions.count(_ == "aes_encrypt") == 1)
      assert(extractedExpressions.count(_ == "uuid") == 1)
    }
  }
}
