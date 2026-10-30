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
import org.apache.spark.sql.catalyst.analysis.FormExternalLookupJoin
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Join, JoinHint,
  LocalRelation, LogicalPlan, PreparedExternalLookupJoin, Project}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.EXTERNAL_LOOKUP
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{ExternalLookup, ExternalLookupReaderFactory, Scan,
  ScanBuilder, SupportsExternalLookup}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation,
  ExternalLookupRelation, PrepareExternalLookupJoin}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PrepareExternalLookupJoinSuite extends PlanTest {
  private val stream = LocalRelation(Seq($"id".int, $"label".string), isStreaming = true)
  private val tableName = "`PlanningExternalLookupTable`"

  private def lookupRelation(table: PlanningExternalLookupTable): ExternalLookupRelation =
    ExternalLookupRelation(
      DataSourceV2Relation.create(table, None, None, CaseInsensitiveStringMap.empty))

  private def lookupJoin(
      lookup: LogicalPlan,
      condition: Expression,
      joinType: JoinType = Inner,
      left: LogicalPlan = stream): LogicalPlan =
    FormExternalLookupJoin(Join(left, lookup, joinType, Some(condition), JoinHint.NONE))

  private def idJoin(lookup: LogicalPlan): LogicalPlan =
    lookupJoin(lookup, stream.output.head === lookup.output.head)

  private def prepare(plan: LogicalPlan): PreparedExternalLookupJoin =
    PrepareExternalLookupJoin(plan) match {
      case lookup: PreparedExternalLookupJoin => lookup
      case other => fail(s"Expected a prepared external lookup join: $other")
    }

  private def prepareError(plan: LogicalPlan): AnalysisException =
    intercept[AnalysisException] { PrepareExternalLookupJoin(plan) }

  test("a lookup-only relation that no lookup join reads is rejected") {
    val lookup = lookupRelation(PlanningExternalLookupTable())
    prepare(idJoin(lookup))
    checkError(
      exception = prepareError(Project(lookup.output, lookup)),
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map("tableName" -> tableName, "operation" -> "batch scan"))
  }

  test("selected predicates bind in the connector's order, and the residual in written order") {
    val Seq(streamId, streamLabel) = stream.output
    def prepared(select: Array[Predicate] => Array[Predicate])
        : (PreparedExternalLookupJoin, Seq[Attribute], Seq[Expression]) = {
      val lookup = lookupRelation(
        PlanningExternalLookupTable("id INT, label STRING, value STRING", select = select))
      val Seq(id, label, value) = lookup.output
      // `value = 'a'` is offered as well; `streamId > 0` is not.
      val conjuncts =
        Seq(id === streamId, streamLabel === label, streamId > 0, value === Literal("a"))
      val join = lookupJoin(lookup, conjuncts.reduce(_ && _), LeftOuter)
      (prepare(join), join.output, conjuncts)
    }

    val (reversed, joinOutput, conjuncts) =
      prepared(candidates => Array(candidates(1), candidates(0)))
    assert(reversed.lookupParameters === Seq(streamLabel, streamId))
    assert(reversed.nullSafeParameters === Seq(false, false))
    assert(reversed.residualCondition === Some(conjuncts(2) && conjuncts(3)))
    assert(reversed.output === joinOutput)
    assert(PrepareExternalLookupJoin(reversed) === reversed)
    checkError(
      exception = intercept[SparkException] { reversed.copy(joinType = FullOuter).output },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Unexpected external lookup join type FullOuter"))

    // An unselected equality stays ahead of the conjuncts written after it.
    val (idOnly, _, idOnlyConjuncts) = prepared(candidates => Array(candidates(0)))
    assert(idOnly.lookupParameters === Seq(streamId))
    assert(idOnly.residualCondition === Some(idOnlyConjuncts.drop(1).reduce(_ && _)))
  }

  test("the read schema binds to the analyzed lookup attributes by exact name") {
    val lookup = lookupRelation(PlanningExternalLookupTable(
      "id INT NOT NULL, value VARCHAR(8) NOT NULL, s STRUCT<a: INT NOT NULL>",
      readSchemaDDL = Some("s STRUCT<a: INT>, value VARCHAR(8), id INT NOT NULL")))
    val join = idJoin(lookup)
    val prepared = prepare(join)
    assert(prepared.lookupOutput === lookup.output)
    assert(prepared.output === join.output)
    // The value-row attributes follow the read schema, with its types and nullability.
    val relationAttrs = lookup.output.map(attr => attr.name -> attr).toMap
    assert(prepared.lookupInput.map(_.name) === Seq("s", "value", "id"))
    assert(prepared.lookupInput.map(_.nullable) === Seq(true, true, false))
    assert(prepared.lookupInput.head.dataType === StructType.fromDDL("a INT"))
    prepared.lookupInput.foreach { attr =>
      assert(attr.exprId === relationAttrs(attr.name).exprId)
      assert(attr.metadata === relationAttrs(attr.name).metadata)
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val caseVariant = lookupRelation(PlanningExternalLookupTable(
        "id INT, a INT, A INT", readSchemaDDL = Some("A INT, id INT, a INT")))
      val Seq(id, a, upperA) = caseVariant.output
      val prepared = prepare(idJoin(caseVariant))
      assert(prepared.lookupInput.map(_.exprId) === Seq(upperA, id, a).map(_.exprId))
    }
  }

  test("the read schema must cover every lookup column with its data type") {
    Seq(
      ("id INT", "MISSING_SCAN_FIELD"),
      ("id INT, value INT", "UNKNOWN_SCAN_FIELD")).foreach { case (readSchema, subClass) =>
      val lookup = lookupRelation(PlanningExternalLookupTable(readSchemaDDL = Some(readSchema)))
      checkError(
        exception = prepareError(idJoin(lookup)),
        condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
        parameters = Map("table" -> tableName, "field" -> "`value`"))
    }
  }

  test("a failed predicate selection blames Spark or the connector, whichever caused it") {
    // A wider stream key casts the lookup column, so Spark has no candidate to offer.
    val longStream = LocalRelation(Seq($"id".long), isStreaming = true)
    val unreachable = lookupRelation(PlanningExternalLookupTable(
      select = _ => fail("Spark must not ask the connector to select from no candidates")))
    checkError(
      exception = prepareError(lookupJoin(unreachable,
        longStream.output.head === unreachable.output.head.cast(LongType), left = longStream)),
      condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
      parameters = Map("table" -> tableName))

    Seq[(Array[Predicate] => Array[Predicate], String)](
      (_ => Array.empty, "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE"),
      (_ => null, "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_PREDICATE_SELECTION"),
      (_ :+ null, "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_PREDICATE_SELECTION")).foreach {
      case (select, condition) =>
        checkError(
          exception = prepareError(idJoin(
            lookupRelation(PlanningExternalLookupTable(select = select)))),
          condition = condition,
          parameters = Map("table" -> tableName))
    }
  }

  test("column pruning never projects the lookup relation under a lookup join") {
    val lookup = lookupRelation(PlanningExternalLookupTable())
    val pruned = ColumnPruning(Project(Seq(stream.output.head), idJoin(lookup)))
    val (lookupLeft, lookupRight) =
      pruned.collectFirst { case join: ExternalLookupJoin => (join.left, join.right) }.get
    assert(lookupLeft.output === Seq(stream.output.head))
    assert(lookupRight === lookup)
    assert(PrepareExternalLookupJoin(pruned).exists(_.isInstanceOf[PreparedExternalLookupJoin]))
  }
}

/**
 * A lookup-only table that the planning rules can prepare. It is never read. By default its
 * connector selects every `id = ?` candidate and returns the table schema as the read schema.
 */
case class PlanningExternalLookupTable(
    schemaDDL: String = "id INT, value STRING NOT NULL",
    readSchemaDDL: Option[String] = None,
    select: Array[Predicate] => Array[Predicate] = PlanningExternalLookupTable.selectId)
  extends Table with SupportsRead {
  override def name(): String = "PlanningExternalLookupTable"
  override def schema(): StructType = StructType.fromDDL(schemaDDL)
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        select(candidates)
      override def build(): Scan = new Scan {
        override def readSchema(): StructType =
          readSchemaDDL.map(StructType.fromDDL).getOrElse(schema())
        override def toExternalLookup(): ExternalLookup = new ExternalLookup {
          override def maxParameterRowsPerRequest(): Int = 1
          override def createReaderFactory(): ExternalLookupReaderFactory =
            () => throw new UnsupportedOperationException("not read")
        }
      }
    }
}

object PlanningExternalLookupTable {
  private val selectId: Array[Predicate] => Array[Predicate] =
    _.filter(p => p.name() == "=" && p.children()(0) == FieldReference.column("id"))
}
