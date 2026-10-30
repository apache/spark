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
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Join, JoinHint,
  LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, EXTERNAL_LOOKUP}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation,
  ExternalLookupRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class FormExternalLookupJoinSuite extends AnalysisTest {
  // The stream side of a stream-static join.
  private val left = LocalRelation(Seq($"tenant".int, $"id".int), isStreaming = true)
  // A lookup-only table has no batch scan, so it reaches the join as this placeholder.
  private val lookupOnly = ExternalLookupRelation(relation(EXTERNAL_LOOKUP))
  private val idCondition = left.output(1) === lookupOnly.output(1)

  private def relation(capabilities: TableCapability*): DataSourceV2Relation =
    DataSourceV2Relation.create(
      LookupCapabilityTable(capabilities.toSet), None, None, CaseInsensitiveStringMap.empty)

  private def join(right: LogicalPlan): Join =
    Join(left, right, Inner, Some(left.output(1) === right.output(1)), JoinHint.NONE)

  // Hints become ResolvedHint nodes, which only Finish Analysis moves into the join.
  private def analyze(plan: LogicalPlan): LogicalPlan =
    getAnalyzer.executeAndCheck(plan, new QueryPlanningTracker)

  test("an eligible join stays a Join in analysis and is converted when optimization starts") {
    val Seq(tenant, id) = left.output
    val compositeKey = tenant === lookupOnly.output(0) && idCondition
    for (condition <- Seq(idCondition, compositeKey); joinType <- Seq(Inner, LeftOuter)) {
      val analyzed = analyze(left.join(lookupOnly, joinType, Some(condition)))
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

  test("keep ordinary joins that cannot become external lookup joins") {
    val other = LocalRelation($"k".int, $"k2".int)
    Seq(
      join(relation(BATCH_READ, EXTERNAL_LOOKUP)),
      join(relation(BATCH_READ)),
      join(other),
      join(Project(lookupOnly.output, lookupOnly))).foreach { join =>
      CheckExternalLookupJoin(join)
      assert(FormExternalLookupJoin(join) == join)
    }
  }

  test("validation and conversion reject a lookup-only join with the same reason") {
    Seq(
      // Only a stream-static join, with the lookup-only table on the right side.
      (left.copy(isStreaming = false).join(lookupOnly, Inner, Some(idCondition)),
        "STATIC_LEFT_SIDE", Map.empty[String, String]),
      (lookupOnly.join(left, Inner, Some(idCondition)), "LOOKUP_TABLE_ON_LEFT",
        Map.empty[String, String]),
      (left.join(lookupOnly, RightOuter, Some(idCondition)), "JOIN_TYPE",
        Map("joinType" -> "RIGHT OUTER")),
      // No join strategy hint applies to an external lookup join; sort merge is named MERGE.
      (left.hint("broadcast").join(lookupOnly, Inner, Some(idCondition)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "BROADCAST")),
      (left.hint("merge").join(lookupOnly, Inner, Some(idCondition)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "MERGE")),
      (left.hint("shuffle_hash").join(lookupOnly, Inner, Some(idCondition)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "SHUFFLE_HASH")),
      (left.hint("shuffle_replicate_nl").join(lookupOnly, Inner, Some(idCondition)),
        "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "SHUFFLE_REPLICATE_NL"))).foreach {
      case (plan, subClass, parameters) =>
        val analyzed = analyze(plan)
        Seq[LogicalPlan => Any](CheckExternalLookupJoin, FormExternalLookupJoin(_)).foreach {
          rule =>
            val error = intercept[AnalysisException] { rule(analyzed) }
            assert(error.getCondition == s"EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.$subClass")
            // The list of supported join types grows in a later change.
            assert(error.getMessageParameters.asScala.toMap - "supportedJoinTypes" ==
              parameters + ("relation" -> "`LookupCapabilityTable`"))
        }
    }
  }

  test("a strategy hint inside a nested join on the stream side belongs to that join") {
    val static = LocalRelation($"k".int)
    val nested =
      left.hint("broadcast").join(static, Inner, Some(left.output(1) === static.output(0)))
    val analyzed = analyze(nested.join(lookupOnly, Inner, Some(idCondition)))
    CheckExternalLookupJoin(analyzed)
    assert(FormExternalLookupJoin(analyzed).isInstanceOf[ExternalLookupJoin])
  }

  test("a join without a condition passes analysis and fails when optimization starts") {
    // Even when a later filter holds the condition, which operator optimization would push
    // into the join.
    Seq(
      left.join(lookupOnly),
      left.join(lookupOnly).where(idCondition),
      left.join(lookupOnly, Cross).where(idCondition)).foreach { plan =>
      val analyzed = analyze(plan)
      // The classic Dataset API analyzes a USING join first as a join without a condition.
      CheckExternalLookupJoin(analyzed)
      checkError(
        exception = intercept[AnalysisException] { FormExternalLookupJoin(analyzed) },
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
        parameters = Map.empty)
    }
  }

  test("validation leaves a join that is not resolved to the analyzer's join checks") {
    // Each join would otherwise fail with JOIN_TYPE.
    val sharedIds = LocalRelation(lookupOnly.output, isStreaming = true)
    Seq(
      Join(left, lookupOnly, RightOuter, Some(left.output(1)), JoinHint.NONE),
      Join(sharedIds, lookupOnly, RightOuter, Some(sharedIds.output(1) === lookupOnly.output(1)),
        JoinHint.NONE)).foreach { join =>
      assert(!join.resolved)
      CheckExternalLookupJoin(join)
    }
  }

  test("an external lookup join node rejects an unsupported join type (internal invariant)") {
    val lookupJoin = FormExternalLookupJoin(join(lookupOnly)).asInstanceOf[ExternalLookupJoin]
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
    StructType.fromDDL("tenant INT NOT NULL, id INT NOT NULL, value STRING NOT NULL")
  override def capabilities(): java.util.Set[TableCapability] = capabilitySet.asJava
}
