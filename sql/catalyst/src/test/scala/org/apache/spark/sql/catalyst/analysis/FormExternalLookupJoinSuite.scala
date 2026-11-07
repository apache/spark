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

  test("keep ordinary joins that cannot become external lookup joins") {
    val other = LocalRelation($"k".int, $"k2".int)
    Seq(
      innerJoinOnId(tableWith(BATCH_READ, EXTERNAL_LOOKUP)),
      innerJoinOnId(tableWith(BATCH_READ)),
      innerJoinOnId(other),
      innerJoinOnId(Project(lookupOnly.output, lookupOnly))).foreach { join =>
      CheckExternalLookupJoin(join)
      assert(FormExternalLookupJoin(join) == join)
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
        Map("joinType" -> "RIGHT OUTER")),
      // A cross join fails with its join type, with or without a later filter.
      (stream.join(lookupOnly, Cross), "JOIN_TYPE", Map("joinType" -> "CROSS")),
      (stream.join(lookupOnly, Cross).where(idsEqual), "JOIN_TYPE", Map("joinType" -> "CROSS")),
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
            // The list of supported join types grows in a later change.
            assert(error.getMessageParameters.asScala.toMap - "supportedJoinTypes" ==
              parameters + ("relation" -> "`LookupCapabilityTable`"))
        }
    }
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
