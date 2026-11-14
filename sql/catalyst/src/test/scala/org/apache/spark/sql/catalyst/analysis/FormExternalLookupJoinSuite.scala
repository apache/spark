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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Rand, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, Distinct,
  ExternalLookupHint, ExternalLookupJoin, Join, JoinHint, LocalRelation, LogicalPlan, Project,
  SubqueryAlias, WithCTE}
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.catalyst.trees.TreePattern.{DATA_SOURCE_V2_RELATION,
  LOOKUP_ONLY_RELATION}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, EXTERNAL_LOOKUP}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation,
  ExternalLookupRelation}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FormExternalLookupJoinSuite extends AnalysisTest {
  // The stream side of a stream-static join.
  private val stream = LocalRelation(Seq($"streamTenant".int, $"streamId".int), isStreaming = true)
  // A lookup-only table has no batch scan, so it reaches the join as this placeholder.
  private val lookupOnly = ExternalLookupRelation(tableWith(EXTERNAL_LOOKUP))
  private val idsEqual = stream.output(1) === lookupOnly.output(1)

  private def tableWith(capabilities: TableCapability*): DataSourceV2Relation =
    DataSourceV2Relation.create(
      LookupCapabilityTable(capabilities.toSet), None, None, CaseInsensitiveStringMap.empty)

  // An inner join of the stream with `right`, on the id column of each.
  private def innerJoinOnId(right: LogicalPlan): Join =
    Join(stream, right, Inner, Some(stream.output(1) === right.output(1)), JoinHint.NONE)

  // The parameters of a JOIN_TYPE error, besides the relation.
  private def joinType(sql: String): Map[String, String] =
    Map("joinType" -> sql, "supportedJoinTypes" -> "INNER, LEFT OUTER, LEFT SEMI, LEFT ANTI")

  // A definition of `plan` named `name`, and a reference to it under that name.
  private def cteOf(
      plan: LogicalPlan,
      name: String,
      materialized: Option[Boolean] = None,
      forceSkipInline: Boolean = false): (CTERelationDef, LogicalPlan) = {
    val cteDef = CTERelationDef(
      SubqueryAlias(name, plan), materialized = materialized, forceSkipInline = forceSkipInline)
    (cteDef, SubqueryAlias(name, CTERelationRef(cteDef.id, _resolved = true, cteDef.output,
      isStreaming = false)))
  }

  // `plan` under the EXTERNAL_LOOKUP hint, without analyzing it.
  private def lookupHint(plan: LogicalPlan): LogicalPlan =
    ResolveExternalLookupHint(plan.hint("external_lookup"))

  // A lookup-only table, and a table that also supports batch reads under the hint.
  private def lookupSides: Seq[(LogicalPlan, LogicalPlan => LogicalPlan)] =
    Seq((lookupOnly, plan => plan), (tableWith(BATCH_READ, EXTERNAL_LOOKUP), lookupHint))

  // Hints become ResolvedHint nodes, which only Finish Analysis moves into the join.
  private def analyze(plan: LogicalPlan): LogicalPlan =
    getAnalyzer.executeAndCheck(plan, new QueryPlanningTracker)

  test("an eligible join stays a Join in analysis and is converted when optimization starts") {
    val Seq(tenant, id) = stream.output
    val compositeKey = tenant === lookupOnly.output(0) && idsEqual
    for (condition <- Seq(idsEqual, compositeKey); joinType <- Seq(Inner, LeftOuter)) {
      val analyzed = analyze(stream.join(lookupOnly, joinType, Some(condition)))
      assert(analyzed.isInstanceOf[Join])
      CheckExternalLookupJoin(analyzed)
      FormExternalLookupJoin(analyzed) match {
        case lookupJoin: ExternalLookupJoin =>
          assert(lookupJoin.joinType == joinType && lookupJoin.condition == Some(condition))
          assert(lookupJoin.requiredLookupOutput == lookupOnly.output)
          assert(lookupJoin.output == analyzed.output)
          assert(lookupJoin.output.drop(2).forall(_.nullable) == (joinType == LeftOuter))
          assert(lookupJoin.missingInput.isEmpty)
          assert(FormExternalLookupJoin(lookupJoin) == lookupJoin)
        case other => fail(s"Expected an external lookup join: $other")
      }
    }
  }

  test("the relation of a lookup-only table carries the lookup-only pattern") {
    Seq(lookupOnly, tableWith(EXTERNAL_LOOKUP)).foreach { relation =>
      assert(relation.containsAllPatterns(DATA_SOURCE_V2_RELATION, LOOKUP_ONLY_RELATION))
      assert(innerJoinOnId(relation).containsPattern(LOOKUP_ONLY_RELATION))
    }
    Seq(tableWith(BATCH_READ, EXTERNAL_LOOKUP), tableWith(BATCH_READ), tableWith()).foreach {
      relation =>
        assert(relation.containsPattern(DATA_SOURCE_V2_RELATION))
        assert(relation.containsPattern(LOOKUP_ONLY_RELATION) === false)
        assert(innerJoinOnId(relation).containsPattern(LOOKUP_ONLY_RELATION) === false)
    }
  }

  test("keep ordinary joins that cannot become external lookup joins") {
    val other = LocalRelation($"k".int, $"k2".int)
    val dual = tableWith(BATCH_READ, EXTERNAL_LOOKUP)
    Seq(
      innerJoinOnId(tableWith(BATCH_READ, EXTERNAL_LOOKUP)),
      innerJoinOnId(tableWith(BATCH_READ)),
      innerJoinOnId(other),
      // Without the hint, a table that also supports batch reads keeps the ordinary join.
      innerJoinOnId(Project(dual.output, dual))).foreach { join =>
      CheckExternalLookupJoin(join)
      assert(FormExternalLookupJoin(join) == join)
    }
  }

  test("the hint opts a join in, and the lookup side is found beneath hints, aliases, " +
      "projections and filters") {
    val dual = tableWith(BATCH_READ, EXTERNAL_LOOKUP)
    Seq(
      dual.hint("external_lookup").as("d"),
      lookupOnly.as("t").hint("external_lookup"),
      dual.select(dual.output: _*).hint("external_lookup"),
      lookupOnly.as("t").where(lookupOnly.output(2) =!= "x").select(lookupOnly.output: _*))
      .foreach { right =>
        val analyzed =
          analyze(stream.join(right, LeftOuter, Some(stream.output(1) === right.output(1))))
        CheckExternalLookupJoin(analyzed)
        val formed = FormExternalLookupJoin(analyzed)
        assert(formed.isInstanceOf[ExternalLookupJoin])
        assert(formed.output == analyzed.output)
        assert(formed.exists(_.isInstanceOf[ExternalLookupHint]) === false)
      }
    // A hint that is not on the lookup side of a join is dropped.
    val stray = analyze(dual.hint("external_lookup"))
    assert(stray.isInstanceOf[ExternalLookupHint])
    assert(FormExternalLookupJoin(stray) == dual)
    // A hint over a set operation without a join below it passes validation too.
    CheckExternalLookupJoin(analyze(dual.union(dual).hint("external_lookup")))
  }

  test("a hint that no join consumes is removed from a plan with a join") {
    val dual = tableWith(BATCH_READ, EXTERNAL_LOOKUP)
    // Next to a join that is formed into a lookup join.
    val lookupJoin = stream.join(lookupOnly, Inner, Some(idsEqual)).select(stream.output(1))
    val besideLookupJoin = lookupJoin.union(lookupHint(dual).select(dual.output(1)))
    // Below a union on the input of a join, where the hint is on the lookup side of no join, so
    // the join stays an ordinary join.
    val belowUnion = innerJoinOnId(lookupHint(dual).union(dual))
    Seq(besideLookupJoin, belowUnion).foreach { plan =>
      assert(plan.exists(_.isInstanceOf[ExternalLookupHint]))
      CheckExternalLookupJoin(plan)
      val formed = FormExternalLookupJoin(plan)
      assert(formed.exists(_.isInstanceOf[ExternalLookupHint]) === false)
      assert(formed.output == plan.output)
    }
    assert(FormExternalLookupJoin(besideLookupJoin).exists(_.isInstanceOf[ExternalLookupJoin]))
    assert(FormExternalLookupJoin(belowUnion) == innerJoinOnId(dual.union(dual)))
  }

  test("validation rejects the hint on a join that reads no DSv2 table") {
    val other = LocalRelation($"k".int, $"k2".int)
    Seq(
      stream.join(other.hint("external_lookup"), Inner, Some(stream.output(1) === other.output(0)))
        -> "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_PLAN",
      stream.hint("external_lookup").join(other, Inner, Some(stream.output(1) === other.output(0)))
        -> "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
      // A hint over the join, through any unary operator, is on the lookup side of no join.
      stream.join(other, Inner, Some(stream.output(1) === other.output(0)))
        .select(other.output(1)).hint("external_lookup")
        -> "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
      // So is a hint over a set operation with a join below one of its inputs.
      stream.join(other, Inner, Some(stream.output(1) === other.output(0)))
        .select(other.output(1)).union(other.select(other.output(1))).hint("external_lookup")
        -> "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE",
      other.select(other.output(1)).except(
        stream.join(other, Inner, Some(stream.output(1) === other.output(0)))
          .select(other.output(1)), isAll = false).hint("external_lookup")
        -> "INVALID_EXTERNAL_LOOKUP_HINT.NOT_ON_LOOKUP_SIDE").foreach { case (join, condition) =>
      val analyzed = analyze(join)
      checkError(
        exception = intercept[AnalysisException](CheckExternalLookupJoin(analyzed)),
        condition = condition,
        parameters = Map.empty)
    }
  }

  test("a lookup side with another operator above the table fails in validation") {
    val dual = tableWith(BATCH_READ, EXTERNAL_LOOKUP)
    // Whether the optimizer would later remove the operator does not matter. A filter with a
    // subquery cannot be evaluated on the lookup results.
    val subqueryFilter =
      lookupOnly.where(lookupOnly.output(1) === ScalarSubquery(LocalRelation($"v".int)))
    // Nor can a non-deterministic projection or filter: the join evaluates it on each returned row,
    // so one stored row could get different values for different stream rows.
    val randomProjection = lookupOnly.select(lookupOnly.output :+ Rand(3L).as("sample"): _*)
    val randomFilter = lookupOnly.where(Rand(3L) >= 0.5)
    Seq(
      lookupOnly.limit(1),
      Distinct(lookupOnly.as("t")),
      dual.limit(1).hint("external_lookup"),
      subqueryFilter,
      randomProjection,
      randomFilter).foreach { right =>
      val analyzed = analyze(stream.join(right, Inner, Some(stream.output(1) === right.output(1))))
      Seq[LogicalPlan => Any](CheckExternalLookupJoin, FormExternalLookupJoin(_)).foreach { rule =>
        checkError(
          exception = intercept[AnalysisException] { rule(analyzed) },
          condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.LOOKUP_PLAN",
          parameters = Map.empty)
      }
    }
  }

  test("validation and conversion reject a lookup-only join with the same reason") {
    Seq(
      // Only a stream-static join, with the lookup-only table on the right side.
      (stream.copy(isStreaming = false).join(lookupOnly, Inner, Some(idsEqual)),
        "STATIC_LEFT_SIDE", Map.empty[String, String]),
      (lookupOnly.join(stream, Inner, Some(idsEqual)), "LOOKUP_TABLE_ON_LEFT",
        Map.empty[String, String]),
      (stream.join(lookupOnly, RightOuter, Some(idsEqual)), "JOIN_TYPE",
        joinType("RIGHT OUTER")),
      // A cross join fails with its join type, with or without a later filter.
      (stream.join(lookupOnly, Cross), "JOIN_TYPE", joinType("CROSS")),
      (stream.join(lookupOnly, Cross).where(idsEqual), "JOIN_TYPE", joinType("CROSS")),
      // No join strategy hint applies to an external lookup join; sort merge is named MERGE.
      (stream.hint("broadcast").join(lookupOnly, Inner, Some(idsEqual)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "BROADCAST")),
      (stream.hint("merge").join(lookupOnly, Inner, Some(idsEqual)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "MERGE")),
      (stream.hint("shuffle_hash").join(lookupOnly, Inner, Some(idsEqual)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "SHUFFLE_HASH")),
      (stream.hint("shuffle_replicate_nl").join(lookupOnly, Inner, Some(idsEqual)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "SHUFFLE_REPLICATE_NL")),
      // The same on the lookup side, where a broadcast hint usually goes in a stream-static join.
      (stream.join(lookupOnly.hint("broadcast"), Inner, Some(idsEqual)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "BROADCAST")),
      (stream.join(lookupOnly.hint("merge"), Inner, Some(idsEqual)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "MERGE")),
      (stream.join(lookupOnly.hint("shuffle_hash"), Inner, Some(idsEqual)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "SHUFFLE_HASH")),
      (stream.join(lookupOnly.hint("shuffle_replicate_nl"), Inner, Some(idsEqual)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "SHUFFLE_REPLICATE_NL"))).foreach {
      case (plan, subClass, parameters) =>
        val analyzed = analyze(plan)
        Seq[LogicalPlan => Any](CheckExternalLookupJoin, FormExternalLookupJoin(_)).foreach {
          rule =>
            val error = intercept[AnalysisException] { rule(analyzed) }
            assert(error.getCondition == s"EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.$subClass")
            assert(error.getMessageParameters.asScala.toMap ==
              parameters + ("relation" -> "`LookupCapabilityTable`"))
        }
    }
  }

  test("a lookup side that holds its own WITH clause is formed with the definition inlined") {
    // As in a DataFrame built from a WITH query, the WithCTE is on the lookup side's path. The
    // reference's alias stays above the inlined definition.
    lookupSides.foreach { case (table, hint) =>
      val (cteDef, ref) = cteOf(table, "d")
      val join = innerJoinOnId(hint(WithCTE(ref, Seq(cteDef))))
      CheckExternalLookupJoin(join)
      FormExternalLookupJoin(join) match {
        case lookupJoin: ExternalLookupJoin =>
          assert(lookupJoin.right == SubqueryAlias("d", SubqueryAlias("d", table)))
        case other => fail(s"Expected an external lookup join: $other")
      }
    }
  }

  test("a lookup side that reads a CTE definition that InlineCTE keeps is rejected") {
    lookupSides.foreach { case (table, hint) =>
      def plan(materialized: Option[Boolean], forceSkipInline: Boolean = false): LogicalPlan = {
        val (cteDef, ref) = cteOf(table, "lk", materialized, forceSkipInline)
        WithCTE(innerJoinOnId(hint(ref)), Seq(cteDef))
      }
      // A MATERIALIZED definition, which CheckAnalysis inlines in the copy that it checks, and
      // one that requests to skip inlining, which it keeps.
      Seq(plan(Some(true)), plan(None, forceSkipInline = true)).foreach { kept =>
        Seq[LogicalPlan => Any](CheckExternalLookupJoin.checkKeptCTEs,
            FormExternalLookupJoin(_)).foreach { rule =>
          checkError(
            exception = intercept[AnalysisException] { rule(kept) },
            condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MATERIALIZED_CTE",
            parameters = Map("cteName" -> "`lk`"))
        }
        assert(ExternalLookupJoinEligibility.lookupSidePlans(kept).nonEmpty)
      }
      checkError(
        exception = intercept[AnalysisException] {
          CheckExternalLookupJoin(plan(None, forceSkipInline = true))
        },
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MATERIALIZED_CTE",
        parameters = Map("cteName" -> "`lk`"))
      // A definition that InlineCTE may inline is inlined into the lookup join.
      Seq(plan(Some(false)), plan(None)).foreach { inlined =>
        CheckExternalLookupJoin(inlined)
        CheckExternalLookupJoin.checkKeptCTEs(inlined)
        val formed = FormExternalLookupJoin(inlined)
        assert(formed.collect { case join: ExternalLookupJoin => join.right } ==
          Seq(SubqueryAlias("lk", SubqueryAlias("lk", table))))
      }
    }
  }

  test("a MATERIALIZED CTE that no lookup side reads keeps InlineCTE's decision") {
    val static = LocalRelation($"k".int)
    val (cteDef, ref) = cteOf(static, "m", materialized = Some(true))
    val lookupJoin = innerJoinOnId(lookupOnly)
    val plan = WithCTE(
      Join(lookupJoin, ref, Inner, Some(stream.output(1) === ref.output(0)), JoinHint.NONE),
      Seq(cteDef))
    CheckExternalLookupJoin(plan)
    CheckExternalLookupJoin.checkKeptCTEs(plan)
    val optimized = InlineCTE()(FormExternalLookupJoin(plan))
    assert(optimized.collect { case join: ExternalLookupJoin => join }.size === 1)
    assert(optimized.collect { case kept: CTERelationDef => kept.id } === Seq(cteDef.id))
    assert(optimized.exists(_.isInstanceOf[CTERelationRef]))
  }

  test("a strategy hint inside a nested join on the stream side belongs to that join") {
    val static = LocalRelation($"k".int)
    val nested =
      stream.hint("broadcast").join(static, Inner, Some(stream.output(1) === static.output(0)))
    val analyzed = analyze(nested.join(lookupOnly, Inner, Some(idsEqual)))
    CheckExternalLookupJoin(analyzed)
    assert(FormExternalLookupJoin(analyzed).isInstanceOf[ExternalLookupJoin])
  }

  test("a join without a condition passes analysis and fails when optimization starts") {
    // Even when a later filter holds the condition, which operator optimization would push
    // into the join.
    Seq(
      stream.join(lookupOnly),
      stream.join(lookupOnly).where(idsEqual)).foreach { plan =>
      val analyzed = analyze(plan)
      // The classic Dataset API analyzes a USING join first as a join without a condition.
      CheckExternalLookupJoin(analyzed)
      checkError(
        exception = intercept[AnalysisException] { FormExternalLookupJoin(analyzed) },
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
        parameters = Map.empty)
    }
  }

  test("validation of a streaming write rejects a join without a condition") {
    def write(query: LogicalPlan): WriteToStream = WriteToStream("query", None, "/checkpoint",
      LookupCapabilityTable(Set.empty), OutputMode.Append(), deleteCheckpointOnStop = false,
      query, None, None)
    Seq(
      stream.join(lookupOnly),
      stream.join(lookupOnly).where(idsEqual)).foreach { plan =>
      val analyzed = analyze(plan)
      // The query of a streaming write is analyzed in full, so the condition cannot come later.
      checkError(
        exception = intercept[AnalysisException] { CheckExternalLookupJoin(write(analyzed)) },
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
        parameters = Map.empty)
    }
    CheckExternalLookupJoin(write(analyze(innerJoinOnId(lookupOnly))))
  }

  test("validation leaves a join that is not resolved to the analyzer's join checks") {
    // Each join would otherwise fail with JOIN_TYPE.
    val sharedIds = LocalRelation(lookupOnly.output, isStreaming = true)
    Seq(
      Join(stream, lookupOnly, RightOuter, Some(stream.output(1)), JoinHint.NONE),
      Join(sharedIds, lookupOnly, RightOuter, Some(sharedIds.output(1) === lookupOnly.output(1)),
        JoinHint.NONE)).foreach { join =>
      assert(!join.resolved)
      CheckExternalLookupJoin(join)
    }
  }

  test("an external lookup join node rejects an unsupported join type (internal invariant)") {
    val lookupJoin =
      FormExternalLookupJoin(innerJoinOnId(lookupOnly)).asInstanceOf[ExternalLookupJoin]
    checkError(
      exception = intercept[SparkException] { lookupJoin.copy(joinType = FullOuter).output },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Unexpected external lookup join type FullOuter"))
  }
}

/** A table with only what the lookup join classification reads: a name, a schema, capabilities. */
private case class LookupCapabilityTable(capabilitySet: Set[TableCapability]) extends Table {
  override def name(): String = "LookupCapabilityTable"
  override def schema(): StructType =
    StructType.fromDDL(
      "lookupTenant INT NOT NULL, lookupId INT NOT NULL, lookupValue STRING NOT NULL")
  override def capabilities(): java.util.Set[TableCapability] = capabilitySet.asJava
}
