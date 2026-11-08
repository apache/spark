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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FormExternalLookupJoin
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet,
  Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Join, JoinHint,
  LocalRelation, LogicalPlan, PreparedExternalLookupJoin, Project}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.EXTERNAL_LOOKUP
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{ExternalLookup, ExternalLookupReader,
  ExternalLookupReaderFactory, Scan, ScanBuilder, SupportsExternalLookup}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation,
  ExternalLookupRelation, PrepareExternalLookupJoin}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

class PrepareExternalLookupJoinSuite extends PlanTest {
  private val stream =
    LocalRelation(Seq($"streamId".int, $"streamLabel".string), isStreaming = true)
  private val QUOTED_TABLE_NAME = "`PlanningExternalLookupTable`"

  private def lookupRelation(table: PlanningExternalLookupTable): ExternalLookupRelation =
    ExternalLookupRelation(
      DataSourceV2Relation.create(table, None, None, CaseInsensitiveStringMap.empty))

  private def lookupJoin(
      lookup: LogicalPlan,
      condition: Expression,
      joinType: JoinType = Inner,
      left: LogicalPlan = stream): LogicalPlan =
    FormExternalLookupJoin(Join(left, lookup, joinType, Some(condition), JoinHint.NONE))

  private def lookupJoinOnId(lookup: LogicalPlan): LogicalPlan =
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
    prepare(lookupJoinOnId(lookup))
    checkError(
      exception = prepareError(Project(lookup.output, lookup)),
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map("tableName" -> QUOTED_TABLE_NAME, "operation" -> "batch scan"))
  }

  test("an equality is a lookup candidate only on a type that stores compare as Spark does") {
    val offeredTypes = Seq("TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "DECIMAL(10, 2)",
      "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "BINARY", "STRING", "STRING COLLATE UTF8_BINARY")
    val notOfferedTypes = Seq("FLOAT", "DOUBLE", "CHAR(3)", "VARCHAR(3)",
      "STRING COLLATE UTF8_LCASE", "STRING COLLATE UNICODE", "STRING COLLATE UTF8_BINARY_RTRIM",
      "STRUCT<a: INT>", "ARRAY<INT>", "INTERVAL DAY TO SECOND")
    for ((codeType, offered) <- offeredTypes.map(_ -> true) ++ notOfferedTypes.map(_ -> false)) {
      var offeredCandidates = Seq.empty[String]
      val lookup = lookupRelation(PlanningExternalLookupTable(s"id INT, code $codeType", select = {
        candidates =>
          offeredCandidates = candidates.map(_.toString).toSeq
          candidates.take(1)
      }))
      val Seq(lookupId, lookupCode) = lookup.output
      val streamId = stream.output.head
      val streamCode = AttributeReference("streamCode", lookupCode.dataType)()
      val codesEqual = streamCode === lookupCode
      val prepared = prepare(lookupJoin(lookup, streamId === lookupId && codesEqual,
        left = LocalRelation(Seq(streamId, streamCode), isStreaming = true)))
      val expected = if (offered) Seq("id = ?", "code = ?") else Seq("id = ?")
      assert(offeredCandidates === expected, codeType)
      // The connector selects only `id = ?`, so Spark evaluates the code equality either way.
      assert(prepared.residualCondition === Some(codesEqual), codeType)
    }

    // Without another candidate, the join cannot run.
    Seq("DOUBLE", "STRING COLLATE UTF8_LCASE").foreach { codeType =>
      val lookup = lookupRelation(PlanningExternalLookupTable(s"id INT, code $codeType"))
      val lookupCode = lookup.output(1)
      val streamCode = AttributeReference("streamCode", lookupCode.dataType)()
      checkError(
        exception = prepareError(lookupJoin(lookup, streamCode === lookupCode,
          left = LocalRelation(Seq(streamCode), isStreaming = true))),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
        parameters = Map("table" -> QUOTED_TABLE_NAME))
    }
  }

  test("the residual evaluates key equalities and conjuncts that cannot fail first") {
    val lookup = lookupRelation(PlanningExternalLookupTable("id INT, label STRING, value STRING"))
    val Seq(lookupId, lookupLabel, lookupValue) = lookup.output
    val Seq(streamId, streamLabel) = stream.output
    // A cast can fail, so it runs after the equality and the comparison written after it, which
    // keep their written order. The connector selects only `id = ?`.
    val mayFail = streamLabel.cast(IntegerType) > 0
    val conjuncts =
      Seq(lookupId === streamId, mayFail, streamLabel === lookupLabel, lookupValue > "a")
    val prepared = prepare(lookupJoin(lookup, conjuncts.reduce(_ && _)))
    assert(prepared.residualCondition ===
      Some(streamLabel === lookupLabel && lookupValue > "a" && mayFail))

    // Equalities between the sides go first even when they can fail, as a hash join's keys: `=`
    // and `<=>`, each comparing a lookup column with a cast of a stream column, written after a
    // conjunct that can fail.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val codes = lookupRelation(PlanningExternalLookupTable("id INT, code INT, alt INT"))
      val Seq(codeId, code, alt) = codes.output
      val codeEqual = code === streamLabel.cast(IntegerType)
      val altEqual = alt <=> streamLabel.cast(IntegerType)
      val keyed = prepare(
        lookupJoin(codes, Seq(codeId === streamId, mayFail, codeEqual, altEqual).reduce(_ && _)))
      assert(keyed.residualCondition === Some(codeEqual && altEqual && mayFail))
    }
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
    val join = lookupJoinOnId(lookup)
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
      val prepared = prepare(lookupJoinOnId(caseVariant))
      assert(prepared.lookupInput.map(_.exprId) === Seq(upperA, id, a).map(_.exprId))
    }
  }

  test("a read schema may declare NOT NULL where the table allows nulls") {
    // A connector that never returns a null may say so, at the top level and in nested fields.
    val lookup = lookupRelation(PlanningExternalLookupTable(
      "id INT, value STRING, s STRUCT<a: INT>",
      readSchemaDDL = Some("id INT, value STRING NOT NULL, s STRUCT<a: INT NOT NULL>")))
    val join = lookupJoinOnId(lookup)
    val prepared = prepare(join)
    assert(prepared.output === join.output)
    assert(prepared.lookupInput.map(_.nullable) === Seq(true, false, true))
    assert(prepared.lookupInput(2).dataType === StructType.fromDDL("a INT NOT NULL"))
  }

  test("a candidate names the lookup column as the table schema spells it") {
    var offered = Seq.empty[String]
    val lookup = lookupRelation(PlanningExternalLookupTable(select = { candidates =>
      offered = candidates.map(_.toString).toSeq
      candidates.filter(_.children()(0) == FieldReference.column("id"))
    }))
    // Under case-insensitive analysis, a condition that writes `ID` resolves to the column `id`,
    // with the same exprId and the query's spelling.
    val idAsWritten = lookup.output.head.withName("ID")
    val prepared = prepare(lookupJoin(lookup, stream.output.head === idAsWritten))
    assert(offered === Seq("id = ?"))
    assert(prepared.lookupParameters === Seq(stream.output.head))
  }

  test("the read schema must cover every lookup column with its data type") {
    Seq(
      ("id INT", "MISSING_SCAN_FIELD"),
      ("id INT, value INT", "UNKNOWN_SCAN_FIELD")).foreach { case (readSchema, subClass) =>
      val lookup = lookupRelation(PlanningExternalLookupTable(readSchemaDDL = Some(readSchema)))
      checkError(
        exception = prepareError(lookupJoinOnId(lookup)),
        condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
        parameters = Map("table" -> QUOTED_TABLE_NAME, "field" -> "`value`"))
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
      parameters = Map("table" -> QUOTED_TABLE_NAME))

    Seq[(Array[Predicate] => Array[Predicate], String)](
      (_ => Array.empty, "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE"),
      (_ => null, "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_PREDICATE_SELECTION"),
      (_ :+ null, "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_PREDICATE_SELECTION")).foreach {
      case (select, condition) =>
        checkError(
          exception = prepareError(lookupJoinOnId(
            lookupRelation(PlanningExternalLookupTable(select = select)))),
          condition = condition,
          parameters = Map("table" -> QUOTED_TABLE_NAME))
    }
  }

  test("a left outer join makes NOT NULL lookup columns nullable") {
    val lookup =
      lookupRelation(PlanningExternalLookupTable("id INT NOT NULL, value STRING NOT NULL"))
    assert(lookup.output.forall(_.nullable === false))
    val join = lookupJoin(lookup, stream.output.head === lookup.output.head, LeftOuter)
    val prepared = prepare(join)
    assert(prepared.output === join.output)
    assert(prepared.output.drop(stream.output.length).forall(_.nullable))
    assert(prepared.copy(joinType = Inner).output.drop(stream.output.length)
      .forall(_.nullable === false))
  }

  test("a prepared lookup join reads its parameters and the residual's stream columns") {
    val lookup = lookupRelation(PlanningExternalLookupTable())
    val Seq(streamId, streamLabel) = stream.output
    val Seq(id, value) = lookup.output
    // The connector selects only `id = ?`, so the residual reads `streamLabel` and `value`.
    val prepared = prepare(lookupJoin(lookup, streamId === id && streamLabel === value))
    assert(prepared.residualCondition === Some(streamLabel === value))
    // It produces `value` itself, so it reads only stream columns.
    assert(prepared.references === AttributeSet(Seq(streamId, streamLabel)))
    assert(prepared.missingInput.isEmpty)
  }

  test("a prepared lookup join keeps its scan and reader factory out of its plan string") {
    val prepared = prepare(lookupJoinOnId(lookupRelation(PlanningExternalLookupTable())))
    assert(prepared.scan.toString.contains("secret-scan"))
    assert(prepared.readerFactory.toString.contains("secret-factory"))
    Seq(prepared.toString, prepared.simpleString(100), prepared.verboseString(100)).foreach {
      rendered => assert(rendered.contains("secret") === false, rendered)
    }
  }

  test("a connector that reorders the offered array in place selects the predicate it returns") {
    val Seq(streamId, streamLabel) = stream.output
    // The connector swaps the two candidates in the array it receives, and returns the one now
    // first, `value = ?`. Spark keeps its own copy, so it binds that predicate to `streamLabel`.
    val lookup = lookupRelation(PlanningExternalLookupTable(select = { candidates =>
      val first = candidates(0)
      candidates(0) = candidates(1)
      candidates(1) = first
      Array(candidates(0))
    }))
    val Seq(id, value) = lookup.output
    val prepared = prepare(lookupJoin(lookup, streamId === id && streamLabel === value))
    assert(prepared.lookupParameters === Seq(streamLabel))
    assert(prepared.residualCondition === Some(streamId === id))
  }

  test("a connector method that returns null fails with a named error") {
    Seq(
      "build" -> "NULL_SCAN",
      "readSchema" -> "NULL_READ_SCHEMA",
      "toExternalLookup" -> "NULL_EXTERNAL_LOOKUP",
      "createReaderFactory" -> "NULL_READER_FACTORY").foreach { case (method, subClass) =>
      val lookup = lookupRelation(PlanningExternalLookupTable(nullFrom = method))
      checkError(
        exception = prepareError(lookupJoinOnId(lookup)),
        condition = s"DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.$subClass",
        parameters = Map("table" -> QUOTED_TABLE_NAME))
    }
  }

  test("a prepared lookup join keeps the size estimate of the join it replaces") {
    // Two stream rows, and a lookup side of Spark's default size.
    withSQLConf(SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "1000") {
      val rows = Seq(1, 2).map(id => InternalRow(id, UTF8String.fromString("a")))
      val sizedStream = LocalRelation(stream.output, rows, isStreaming = true)
      val lookup = lookupRelation(PlanningExternalLookupTable())
      val join = lookupJoin(lookup, sizedStream.output.head === lookup.output.head,
        left = sizedStream)
      val prepared = prepare(join)
      assert(sizedStream.stats.sizeInBytes > 0)
      assert(prepared.stats.sizeInBytes === sizedStream.stats.sizeInBytes * 1000)
      assert(prepared.stats.sizeInBytes === join.stats.sizeInBytes)
    }
  }

  test("column pruning never projects the lookup relation under a lookup join") {
    val lookup = lookupRelation(PlanningExternalLookupTable())
    val pruned = ColumnPruning(Project(Seq(stream.output.head), lookupJoinOnId(lookup)))
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
    select: Array[Predicate] => Array[Predicate] = PlanningExternalLookupTable.selectIdEqualities,
    nullFrom: String = "")
  extends Table with SupportsRead {
  override def name(): String = "PlanningExternalLookupTable"
  override def schema(): StructType = StructType.fromDDL(schemaDDL)
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        select(candidates)
      override def build(): Scan = if (nullFrom == "build") null else new Scan {
        override def readSchema(): StructType = if (nullFrom == "readSchema") null else {
          readSchemaDDL.map(StructType.fromDDL).getOrElse(schema())
        }
        override def toExternalLookup(): ExternalLookup =
          if (nullFrom == "toExternalLookup") null else new ExternalLookup {
            override def maxParameterRowsPerRequest(): Int = 1
            override def createReaderFactory(): ExternalLookupReaderFactory =
              if (nullFrom == "createReaderFactory") null else new ExternalLookupReaderFactory {
                override def createReader(): ExternalLookupReader =
                  throw new UnsupportedOperationException("not read")
                // A connector description may hold a secret.
                override def toString: String = "PlanningReaderFactory(token=secret-factory)"
              }
          }
        override def toString: String = "PlanningScan(token=secret-scan)"
      }
    }
}

object PlanningExternalLookupTable {
  private val selectIdEqualities: Array[Predicate] => Array[Predicate] =
    _.filter(p => p.name() == "=" && p.children()(0) == FieldReference.column("id"))
}
