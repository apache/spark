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

package org.apache.spark.sql.connector

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.ResolveExternalLookupJoin
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LocalRelation,
  PreparedExternalLookupJoin, Project}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.EXTERNAL_LOOKUP
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{ExternalLookup, ExternalLookupReaderFactory, Scan,
  ScanBuilder, SupportsExternalLookup}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation,
  PrepareExternalLookupJoin}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PrepareExternalLookupJoinSuite extends QueryTest with SharedSparkSession {

  test("a lookup-only relation that no lookup join reads is rejected") {
    val left = LocalRelation(Seq(AttributeReference("id", IntegerType)()), isStreaming = true)
    val relation = DataSourceV2Relation.create(
      PlanningExternalLookupTable, None, None, CaseInsensitiveStringMap.empty)
    val lookupJoin = ResolveExternalLookupJoin(Join(
      left, relation, Inner, Some(EqualTo(left.output.head, relation.output.head)), JoinHint.NONE))

    assert(PrepareExternalLookupJoin(lookupJoin).isInstanceOf[PreparedExternalLookupJoin])
    checkError(
      exception = intercept[AnalysisException] {
        PrepareExternalLookupJoin(Project(relation.output, relation))
      },
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map(
        "tableName" -> "`PlanningExternalLookupTable`",
        "operation" -> "batch scan"))
  }

  test("PrepareExternalLookupJoin binds the selected predicates and keeps the rest as residual") {
    val left = LocalRelation(Seq(AttributeReference("id", IntegerType)()), isStreaming = true)
    val relation = DataSourceV2Relation.create(
      PlanningExternalLookupTable, None, None, CaseInsensitiveStringMap.empty)
    val Seq(id, value) = relation.output
    // Offered to the connector as `value = ?`, but not selected.
    val unselected = EqualTo(value, Literal("a"))
    val join = Join(left, relation, LeftOuter, Some(And(EqualTo(left.output.head, id), unselected)),
      JoinHint.NONE)

    val prepared = PrepareExternalLookupJoin(ResolveExternalLookupJoin(join))
    prepared match {
      case lookup: PreparedExternalLookupJoin =>
        assert(lookup.lookupParameters === left.output)
        assert(lookup.residualCondition === Some(unselected))
        assert(lookup.output === join.output)
        checkError(
          exception = intercept[SparkException] { lookup.copy(joinType = FullOuter).output },
          condition = "INTERNAL_ERROR",
          parameters = Map("message" -> "Unexpected external lookup join type FullOuter"))
      case other => fail(s"Expected a prepared external lookup join: $other")
    }
    assert(PrepareExternalLookupJoin(prepared) === prepared)
  }

  test("a join without a condition over a lookup-only table fails on the final plan") {
    val left = LocalRelation(Seq(AttributeReference("id", IntegerType)()), isStreaming = true)
    val relation = DataSourceV2Relation.create(
      PlanningExternalLookupTable, None, None, CaseInsensitiveStringMap.empty)
    // Analysis keeps such a join, because the Dataset API analyzes a USING join first as one.
    checkError(
      exception = intercept[AnalysisException] {
        PrepareExternalLookupJoin(ResolveExternalLookupJoin(
          Join(left, relation, Inner, None, JoinHint.NONE)))
      },
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.MISSING_JOIN_CONDITION",
      parameters = Map.empty)
  }
}

/** A lookup-only table that the planning rules can prepare. It is never read. */
object PlanningExternalLookupTable extends Table with SupportsRead {
  override def name(): String = "PlanningExternalLookupTable"
  override def schema(): StructType = StructType.fromDDL("id INT, value STRING NOT NULL")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        candidates.filter(p => p.name() == "=" && p.children()(0) == FieldReference.column("id"))
      override def build(): Scan = new Scan {
        override def readSchema(): StructType = schema()
        override def toExternalLookup(): ExternalLookup = new ExternalLookup {
          override def maxParameterRowsPerRequest(): Int = 1
          override def createReaderFactory(): ExternalLookupReaderFactory =
            () => throw new UnsupportedOperationException("not read")
        }
      }
    }
}
