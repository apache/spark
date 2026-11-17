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

package org.apache.spark.sql.execution

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.MapObjects
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, Project}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.streaming.checkpointing.OffsetSeqMetadata
import org.apache.spark.sql.execution.streaming.runtime.{IncrementalExecution, MemoryStream, WatermarkPropagator}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class KllStreamingAggregationPlanningSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val legacySharingKey = "spark.sql.streaming.aggregation.legacyKllSharing.enabled"

  test("corrected checkpoint policy recovers independently written static HAVING calls") {
    val logical = withSQLConf(SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> "true") {
      val input = MemoryStream[Long](spark).toDF()
      val aggregate = "kll_sketch_agg_bigint(id)"
      val scalar = s"""(SELECT kll_sketch_get_n_bigint(s) FROM
          |(SELECT $aggregate AS s FROM VALUES (0L), (1L), (2L) AS t(id)
          |HAVING $aggregate = s))""".stripMargin
      val result = input.agg(expr(s"sum($scalar)").as("total"))
      result.queryExecution.analyzed
    }
    withSQLConf(legacySharingKey -> "false") {
      val execution = new IncrementalExecution(spark, logical, OutputMode.Complete(),
        "/tmp/unused-kll-static-policy", UUID.randomUUID(), UUID.randomUUID(), 0L, None,
        OffsetSeqMetadata(0L, 0L, Map.empty[String, String]), WatermarkPropagator.noop(),
        isFirstBatch = true)
      val occurrences = execution.normalized.collectWithSubqueries {
        case aggregate: Aggregate => aggregate.aggregateExpressions.flatMap(_.collect {
          case k: KllSketchAggBigint => k
        })
      }.flatten
      assert(occurrences.size == 2, execution.normalized.treeString)
      assert(occurrences.forall(!_.deterministic))
      val comparisons = execution.normalized.collectWithSubqueries {
        case org.apache.spark.sql.catalyst.plans.logical.Filter(
            EqualTo(left: Attribute, right: Attribute), _) => (left.exprId, right.exprId)
      }
      assert(comparisons.size == 1, execution.normalized.treeString)
      assert(comparisons.head._1 != comparisons.head._2, execution.normalized.treeString)
    }
  }

  test("legacy checkpoint retires unchosen HAVING buffers without column pruning") {
    for (analysisLegacy <- Seq(false, true)) {
      val logical = withSQLConf(
          SQLConf.LEGACY_KLL_AGGREGATE_DETERMINISTIC.key -> analysisLegacy.toString) {
        val input = MemoryStream[(Long, Long, Long, Long)](spark).toDF()
          .toDF("a", "b", "c", "d")
        input.createOrReplaceTempView("kll_unchosen_buffers")
        try {
          spark.sql("""SELECT kll_sketch_agg_bigint(((a+b)+c)+d) AS s
              |FROM kll_unchosen_buffers
              |HAVING kll_sketch_get_n_bigint(kll_sketch_agg_bigint((a+b)+(c+d))) > 0
              |""".stripMargin).queryExecution.analyzed
        } finally {
          spark.catalog.dropTempView("kll_unchosen_buffers")
        }
      }
      withSQLConf(legacySharingKey -> "true",
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
            "org.apache.spark.sql.catalyst.optimizer.ColumnPruning") {
        val execution = new IncrementalExecution(spark, logical, OutputMode.Complete(),
          "/tmp/unused-kll-unchosen-buffers", UUID.randomUUID(), UUID.randomUUID(), 0L, None,
          OffsetSeqMetadata(0L, 0L, Map.empty[String, String]), WatermarkPropagator.noop(),
          isFirstBatch = true)
        val aggregate = execution.optimizedPlan.collect { case a: Aggregate => a }.head
        val functions = org.apache.spark.sql.catalyst.planning.PhysicalAggregation
          .unapply(aggregate).get._2
        assert(functions.size == 1, execution.optimizedPlan.treeString)
      }
    }
  }

  test("normalization preserves compiler binding recovery through the micro-batch lifecycle") {
    for (removeProducer <- Seq(false, true); legacy <- Seq(false, true)) {
      val session = new org.apache.spark.sql.classic.SparkSession(spark.sparkContext)
      val normalizer = new org.apache.spark.sql.catalyst.rules.Rule[
          org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] {
        override def apply(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan)
            : org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = {
          plan.transformAllExpressionsWithSubqueries {
            case scalar: ScalarSubquery if removeProducer && scalar.plan.containsPattern(
                org.apache.spark.sql.catalyst.trees.TreePattern.KLL_AGGREGATE) => Literal(3L)
            case alias: Alias if alias.isNondeterministicSubqueryBinding =>
              Alias(alias.child, alias.name)(alias.exprId, alias.qualifier,
                alias.explicitMetadata, alias.nonInheritableMetadataKeys)
          }
        }
      }
      session.extensions.injectPlanNormalizationRule(_ => normalizer)
      session.conf.set(legacySharingKey, legacy.toString)
      val scalar = """(SELECT kll_sketch_get_n_bigint(kll_sketch_agg_bigint(id))
          |FROM VALUES (0L), (1L), (2L) AS t(id))""".stripMargin
      val input = MemoryStream[Long](spark).toDF().withColumn("user", expr(scalar))
      val userId = input.queryExecution.analyzed.output.last.exprId
      val logical = input.agg(expr(s"sum($scalar)").as("a"),
        expr(s"sum($scalar)").as("b")).queryExecution.analyzed
      val execution = new IncrementalExecution(session, logical, OutputMode.Complete(),
        "/tmp/unused-kll-normalizer", UUID.randomUUID(), UUID.randomUUID(), 0L, None,
        OffsetSeqMetadata(0L, 0L, Map.empty[String, String]), WatermarkPropagator.noop(),
        isFirstBatch = true)
      val normalized = execution.normalized
      assert(normalized.exists {
        case project: Project => project.projectList.exists(_.exprId == userId)
        case _ => false
      }, normalized.treeString)
      val optimized = execution.optimizedPlan
      val aggregates = optimized.collect { case a: Aggregate => a }
      assert(aggregates.size == 1, optimized.treeString)
      val functions = org.apache.spark.sql.catalyst.planning.PhysicalAggregation
        .unapply(aggregates.head).get._2
      assert(functions.size == (if (legacy) 1 else 2),
        s"removeProducer=$removeProducer legacy=$legacy\n$optimized")
    }
  }

  private case class NormalizationCounted(child: Expression, visits: AtomicInteger)
      extends Expression with Unevaluable {
    override def children: Seq[Expression] = {
      visits.incrementAndGet()
      Seq(child)
    }
    override def dataType: org.apache.spark.sql.types.DataType = LongType
    override def nullable: Boolean = false
    override protected def withNewChildrenInternal(children: IndexedSeq[Expression]): Expression =
      copy(child = children.head)
  }

  test("ordinary micro-batch normalization skips the KLL adjustment walk") {
    for (batch <- 0L to 2L; legacy <- Seq(false, true); aggregate <- Seq(false, true);
        subquery <- Seq(false, true)) {
      withSQLConf(legacySharingKey -> legacy.toString) {
        def work(incremental: Boolean): Int = {
          val visits = new AtomicInteger
          val value = AttributeReference("value", LongType)()
          val counted = (1 to 64).foldLeft[Expression](value) { (child, _) =>
            NormalizationCounted(child, visits)
          }
          val expression = if (subquery) If(ScalarSubquery(Project(
            Seq(Alias(Literal.TrueLiteral, "b")()),
            org.apache.spark.sql.catalyst.plans.logical.OneRowRelation())), counted, Literal(0L))
          else counted
          val plan = if (aggregate) Aggregate(Nil,
            Seq(Alias(Sum(expression).toAggregateExpression(), "total")()), LocalRelation(value))
          else Project(Seq(Alias(expression, "mapped")()), LocalRelation(value))
          val execution: QueryExecution = if (!incremental) new QueryExecution(spark, plan)
          else new IncrementalExecution(spark, plan, OutputMode.Append(),
            "/tmp/unused-kll-normalization-checkpoint", UUID.randomUUID(), UUID.randomUUID(),
            batch, None, OffsetSeqMetadata(0L, 0L, Map.empty[String, String]),
            WatermarkPropagator.noop(),
            isFirstBatch = batch == 0)
          execution.tableVersionsRefreshed.treePatternBits
          visits.set(0)
          execution.normalized
          visits.get
        }
        val baseline = work(incremental = false)
        val actual = work(incremental = true)
        assert(actual <= baseline,
          s"batch=$batch legacy=$legacy aggregate=$aggregate " +
            s"baseline visits=$baseline incremental visits=$actual")
      }
    }
  }

  private def checkPlan(
      logical: Aggregate,
      legacySharing: Boolean,
      retained: Seq[Int],
      results: Seq[Int]): Unit = {
    val original = logical.aggregateExpressions.flatMap(_.collect {
      case a: AggregateExpression => a
    })
    withSQLConf(
      legacySharingKey -> legacySharing.toString,
      SQLConf.STREAMING_USE_STREAMLINE_AGGREGATOR.key -> "false") {
      val planned = spark.sessionState.planner.StatefulAggregationStrategy(logical)
      assert(planned.size == 1)
      val aggregates = planned.head.collect { case a: BaseAggregateExec => a }
      assert(aggregates.nonEmpty)
      // Every stage must use the same buffer order, including aggregates between KLL calls.
      aggregates.foreach { aggregate =>
        assert(aggregate.aggregateExpressions.map(_.resultId) == retained.map(original(_).resultId))
      }
      val finalAggregate = aggregates.head
      assert(finalAggregate.output.map(_.exprId) == logical.output.map(_.exprId))
      assert(finalAggregate.resultExpressions.map(_.references.iterator.map(_.exprId).toSet) ==
        results.map(index => Set(original(index).resultId)))
    }
  }

  for (valueType <- Seq("bigint", "float", "double");
      merge <- Seq(false, true);
      k <- Seq("", ", 8")) {
    val function = if (merge) s"kll_merge_agg_$valueType" else s"kll_sketch_agg_$valueType"
    test(s"$function with k='$k' shares buffers only for legacy streaming queries") {
      val input = if (merge) MemoryStream[Array[Byte]](spark).toDF()
        else MemoryStream[Long](spark).toDF()
      val argument = if (merge) "value" else s"CAST(value AS $valueType)"
      val logical = input.agg(
        expr(s"$function($argument$k)").as("a"),
        expr(s"$function($argument$k)").as("b"))
        .queryExecution.analyzed.asInstanceOf[Aggregate]
      checkPlan(logical, legacySharing = false, retained = Seq(0, 1), results = Seq(0, 1))
      checkPlan(logical, legacySharing = true, retained = Seq(0), results = Seq(0, 0))
    }
  }

  test("legacy KLL sharing preserves interleaved buffers and distinct k and filters") {
    val input = MemoryStream[Long](spark).toDF()
    val filtered = "kll_sketch_agg_bigint(value, 8) FILTER (WHERE value > 0)"
    val logical = input.agg(
      expr(filtered).as("a"),
      expr("count(*)").as("n"),
      expr(filtered).as("b"),
      expr("kll_sketch_agg_bigint(value, 16) FILTER (WHERE value > 0)").as("c"),
      expr("count(*)").as("m"),
      expr("kll_sketch_agg_bigint(value, 8) FILTER (WHERE value < 0)").as("d"))
      .queryExecution.analyzed.asInstanceOf[Aggregate]
    checkPlan(logical, legacySharing = false,
      retained = Seq(0, 1, 2, 3, 5), results = Seq(0, 1, 2, 3, 1, 5))
    checkPlan(logical, legacySharing = true,
      retained = Seq(0, 1, 3, 5), results = Seq(0, 1, 0, 3, 1, 5))
  }

  test("legacy KLL sharing preserves repeated references to one aggregate result") {
    val input = MemoryStream[Long](spark).toDF().queryExecution.analyzed
    val aggregate = KllSketchAggBigint(input.output.head).toAggregateExpression()
    val logical = Aggregate(Nil, Seq(Alias(aggregate, "a")(), Alias(aggregate, "b")()), input)
    checkPlan(logical, legacySharing = true, retained = Seq(0), results = Seq(0, 0))
  }

  test("legacy KLL sharing does not combine nondeterministic inputs or scoped object mappings") {
    val input = MemoryStream[Long](spark).toDF().queryExecution.analyzed
    val value = input.output.head
    val mapped = Size(MapObjects(identity[Expression], CreateArray(Seq(value)), LongType,
      elementNullable = false))
    val cases = Seq[(Function0[AggregateFunction], Option[Expression])](
      (() => KllSketchAggDouble(Rand(1L)), None),
      (() => KllSketchAggBigint(value), Some(GreaterThan(Rand(1L), Literal(0.5d)))),
      (() => KllSketchAggBigint(mapped), None))

    cases.foreach { case (function, filter) =>
      // Construct resolved planner inputs directly: analysis rejects random aggregate inputs,
      // but the compatibility planner must not make them eligible for historical sharing.
      val logical = Aggregate(Nil, Seq("a", "b").map { name =>
        Alias(function().toAggregateExpression(isDistinct = false, filter = filter), name)()
      }, input)
      assert(logical.resolved)
      checkPlan(logical, legacySharing = true, retained = Seq(0, 1), results = Seq(0, 1))
    }
  }

  test("legacy KLL sharing preserves separate buffers for different expression heights") {
    val input = MemoryStream[(Long, Long, Long, Long)](spark).toDF().queryExecution.analyzed
    val Seq(a, b, c, d) = input.output
    val leftDeep = Add(Add(Add(a, b), c), d)
    val balanced = Add(Add(a, b), Add(c, d))
    assert(leftDeep.canonicalized == balanced.canonicalized)
    assert(leftDeep.height != balanced.height)
    val logical = Aggregate(Nil, Seq(leftDeep, balanced).zipWithIndex.map { case (child, i) =>
      Alias(KllSketchAggBigint(child).toAggregateExpression(), s"sketch_$i")()
    }, input)
    checkPlan(logical, legacySharing = true, retained = Seq(0, 1), results = Seq(0, 1))
  }
}
