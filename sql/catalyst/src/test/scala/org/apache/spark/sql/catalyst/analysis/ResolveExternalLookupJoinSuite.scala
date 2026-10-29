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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, ExternalLookupJoin, HintInfo, Join,
  JoinHint, JoinStrategyHint, LocalRelation, LogicalPlan, SHUFFLE_HASH, SHUFFLE_MERGE,
  SHUFFLE_REPLICATE_NL}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, EXTERNAL_LOOKUP}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ResolveExternalLookupJoinSuite extends AnalysisTest {
  // The stream side of a stream-static join.
  private val left = LocalRelation(Seq($"tenant".int, $"id".int), isStreaming = true)
  private val lookupOnly = relation(EXTERNAL_LOOKUP)

  private def relation(capabilities: TableCapability*): DataSourceV2Relation =
    DataSourceV2Relation.create(
      LookupCapabilityTable(capabilities.toSet), None, None, CaseInsensitiveStringMap.empty)

  private def join(right: LogicalPlan): Join =
    Join(left, right, Inner, Some(left.output(1) === right.output(1)), JoinHint.NONE)

  test("rewrite inner and left outer equality joins over lookup-only tables") {
    val Seq(tenant, id) = left.output
    val singleKey = id === lookupOnly.output(1)
    val compositeKey = tenant === lookupOnly.output(0) && singleKey
    for (condition <- Seq(singleKey, compositeKey); joinType <- Seq(Inner, LeftOuter)) {
      val join = Join(left, lookupOnly, joinType, Some(condition), JoinHint.NONE)
      val resolved = ResolveExternalLookupJoin(join)
      resolved match {
        case lookupJoin: ExternalLookupJoin =>
          assert(lookupJoin.joinType == joinType && lookupJoin.condition == Some(condition))
          assert(lookupJoin.requiredLookupOutput == lookupOnly.output)
        case other => fail(s"Expected an external lookup join: $other")
      }
      assert(resolved.output == join.output)
      assert(resolved.output.drop(2).forall(_.nullable) == (joinType == LeftOuter))
      assert(resolved.missingInput.isEmpty)
      assert(ResolveExternalLookupJoin(resolved) == resolved)
    }
  }

  test("keep ordinary joins that cannot become external lookup joins") {
    val other = LocalRelation($"k".int, $"k2".int)
    Seq(
      join(relation(BATCH_READ, EXTERNAL_LOOKUP)),
      join(relation(BATCH_READ)),
      join(other),
      join(Join(other.copy(isStreaming = true), lookupOnly, Cross, None, JoinHint.NONE)),
      // A missing condition is reported on the final plan, because the Dataset API analyzes a
      // USING join first as a join without a condition.
      Join(left, lookupOnly, Inner, None, JoinHint.NONE)).foreach { join =>
      assert(ResolveExternalLookupJoin(join) == join)
    }
  }

  test("reject lookup-only joins that cannot run as external lookup joins") {
    def hinted(strategy: JoinStrategyHint): Join =
      join(lookupOnly).copy(hint = JoinHint(Some(HintInfo(strategy = Some(strategy))), None))
    Seq(
      // Only a stream-static join, with the lookup-only table on the right side.
      (join(lookupOnly).copy(left = left.copy(isStreaming = false)), "STATIC_LEFT_SIDE",
        Map.empty[String, String]),
      (Join(lookupOnly, left, Inner, Some(lookupOnly.output(1) === left.output(1)), JoinHint.NONE),
        "LOOKUP_TABLE_ON_LEFT", Map.empty[String, String]),
      (join(lookupOnly).copy(joinType = RightOuter), "JOIN_TYPE", Map("joinType" -> "RIGHT OUTER")),
      // No join strategy hint applies to an external lookup join; sort merge is named MERGE.
      (hinted(BROADCAST), "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "BROADCAST")),
      (hinted(SHUFFLE_MERGE), "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "MERGE")),
      (hinted(SHUFFLE_HASH), "CONFLICTING_JOIN_STRATEGY", Map("strategy" -> "SHUFFLE_HASH")),
      (hinted(SHUFFLE_REPLICATE_NL), "CONFLICTING_JOIN_STRATEGY",
        Map("strategy" -> "SHUFFLE_REPLICATE_NL"))).foreach { case (join, subClass, parameters) =>
      val error = intercept[AnalysisException] { ResolveExternalLookupJoin(join) }
      assert(error.getCondition == s"EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.$subClass")
      // The list of supported join types grows in a later layer.
      assert(error.getMessageParameters.asScala.toMap - "supportedJoinTypes" ==
        parameters + ("relation" -> "`LookupCapabilityTable`"))
    }
  }

  test("an external lookup join node rejects an unsupported join type (internal invariant)") {
    val lookupJoin = ResolveExternalLookupJoin(join(lookupOnly)).asInstanceOf[ExternalLookupJoin]
    checkError(
      exception = intercept[SparkException] { lookupJoin.copy(joinType = FullOuter).output },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Unexpected external lookup join type FullOuter"))
  }
}

/** A table with only what ResolveExternalLookupJoin reads: a name, a schema and capabilities. */
private case class LookupCapabilityTable(capabilitySet: Set[TableCapability]) extends Table {
  override def name(): String = "LookupCapabilityTable"
  override def schema(): StructType =
    StructType.fromDDL("tenant INT NOT NULL, id INT NOT NULL, value STRING NOT NULL")
  override def capabilities(): java.util.Set[TableCapability] = capabilitySet.asJava
}
