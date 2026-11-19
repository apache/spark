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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FormExternalLookupJoin
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap,
  AttributeReference, AttributeSet, BindReferences, Cast, Coalesce, EqualTo, EvalMode, Expression,
  ExternalUserDefinedFunction, Literal, NamedExpression, Not, Predicate => CatalystPredicate, Upper}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi,
  PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{ExecuteExternalUDF, ExternalLookupFilter,
  ExternalLookupJoin, ExternalLookupProject, ExternalLookupTransform, Filter, Join, JoinHint,
  LocalRelation, LogicalPlan, LookupKeyComparison, PreparedExternalLookupJoin, Project}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.EXTERNAL_LOOKUP
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{ExternalLookup, ExternalLookupReader,
  ExternalLookupReaderFactory, Scan, ScanBuilder, SupportsExternalLookup}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation,
  ExternalLookupRelation, PrepareExternalLookupJoin}
import org.apache.spark.sql.execution.externalUDF.PlanExternalUDFs
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DateType, DecimalType, IntegerType,
  LongType, MapType, StringType, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.DataSourceV2Test
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.unsafe.types.UTF8String

@DataSourceV2Test
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

  // A conjunct of the residual's first group, read as false when it is NULL.
  private def orFalse(conjunct: Expression): Expression =
    Coalesce(Seq(conjunct, Literal.FalseLiteral))

  /**
   * Whether Spark offers `code = ?` on a lookup column `code` of the given type. The connector
   * selects only `id = ?`, so Spark evaluates the code equality either way.
   */
  private def offeredCodeCandidate(codeType: String): Boolean = {
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
    assert(prepared.residualCondition === Some(codesEqual), codeType)
    offeredCandidates match {
      case Seq("id = ?") => false
      case Seq("id = ?", "code = ?") => true
      case other => fail(s"Unexpected candidates for $codeType: $other")
    }
  }

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
      "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "BINARY", "STRING", "STRING COLLATE UTF8_BINARY",
      "VARCHAR(3)")
    val notOfferedTypes = Seq("FLOAT", "DOUBLE", "CHAR(3)", "VARCHAR(3) COLLATE UTF8_LCASE",
      "STRING COLLATE UTF8_LCASE", "STRING COLLATE UNICODE", "STRING COLLATE UTF8_BINARY_RTRIM",
      "STRUCT<a: INT>", "ARRAY<INT>", "INTERVAL DAY TO SECOND")
    for ((codeType, offered) <- offeredTypes.map(_ -> true) ++ notOfferedTypes.map(_ -> false)) {
      assert(offeredCodeCandidate(codeType) === offered, codeType)
    }
    // Under standard CHAR/VARCHAR semantics, Spark reads a VARCHAR value longer than the declared
    // length without the trailing blanks past that length, so a store that compares the value as
    // stored could miss a row.
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      assert(offeredCodeCandidate("VARCHAR(3)") === false)
      assert(offeredCodeCandidate("STRING"))
    }

    // Without another candidate, the join cannot run.
    Seq("DOUBLE", "STRING COLLATE UTF8_LCASE", "CHAR(3)").foreach { codeType =>
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

  test("an equality within one side of the join is never a lookup candidate") {
    var offered = Seq.empty[String]
    val lookup = lookupRelation(PlanningExternalLookupTable("id INT, value STRING, alt STRING",
      select = { candidates =>
        offered = candidates.map(_.toString).toSeq
        candidates.take(1)
      }))
    val Seq(id, value, alt) = lookup.output
    val streamCode = AttributeReference("streamCode", value.dataType)()
    val left = LocalRelation(stream.output :+ streamCode, isStreaming = true)
    val Seq(streamId, streamLabel) = stream.output
    // `t.value = t.alt` would offer a parameter over a lookup column, and `s.label = s.code` a
    // candidate on a column that the lookup side does not have.
    val withinSides = Seq(value === alt, alt === value, streamLabel === streamCode)
    val prepared = prepare(
      lookupJoin(lookup, ((streamId === id) +: withinSides).reduce(_ && _), left = left))
    assert(offered === Seq("id = ?"))
    assert(prepared.lookupParameters === Seq(streamId))
    assert(prepared.residualCondition === Some(withinSides.reduce(_ && _)))
  }

  test("the residual evaluates key equalities and conjuncts that cannot fail first") {
    // With ANSI mode on, a cast of a string to INT fails on a string that is not a number. Without
    // it, ExprUtils.canEvaluateUnconditionally may take the cast as unable to fail.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val lookup = lookupRelation(PlanningExternalLookupTable("id INT, label STRING, value STRING"))
      val Seq(lookupId, lookupLabel, lookupValue) = lookup.output
      val Seq(streamId, streamLabel) = stream.output
      // The cast can fail, so it runs after the equality and the comparison written after it,
      // which keep their written order. The connector selects only `id = ?`.
      val mayFail = streamLabel.cast(IntegerType) > 0
      val conjuncts =
        Seq(lookupId === streamId, mayFail, streamLabel === lookupLabel, lookupValue > "a")
      val prepared = prepare(lookupJoin(lookup, conjuncts.reduce(_ && _)))
      assert(prepared.residualCondition ===
        Some(orFalse(streamLabel === lookupLabel) && orFalse(lookupValue > "a") && mayFail))

      // Equalities between the sides go first even when they can fail, as a hash join's keys: `=`
      // and `<=>`, each comparing a lookup column with a cast of a stream column, written after a
      // conjunct that can fail. A `<=>` is never NULL.
      val codes = lookupRelation(PlanningExternalLookupTable("id INT, code INT, alt INT"))
      val Seq(codeId, code, alt) = codes.output
      val codeEqual = code === streamLabel.cast(IntegerType)
      val altEqual = alt <=> streamLabel.cast(IntegerType)
      val keyed = prepare(
        lookupJoin(codes, Seq(codeId === streamId, mayFail, codeEqual, altEqual).reduce(_ && _)))
      assert(keyed.residualCondition === Some(orFalse(codeEqual) && altEqual && mayFail))
    }
  }

  test("a NULL key equality in the residual stops the conjuncts that can fail") {
    val lookup = lookupRelation(PlanningExternalLookupTable("id INT, label STRING"))
    val Seq(lookupId, lookupLabel) = lookup.output
    val Seq(streamId, streamLabel) = stream.output
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // Under ANSI mode, the cast fails on a label that is not a number.
      val mayFail = streamLabel.cast(IntegerType) > 0
      val keyEqual = streamLabel === lookupLabel
      val prepared = prepare(lookupJoin(lookup, lookupId === streamId && keyEqual && mayFail))
      val joinedOutput = stream.output ++ lookup.output
      val residual = prepared.residualCondition.get
      val conjunction = keyEqual && mayFail
      def row(label: String, lookupLabelValue: String): InternalRow = InternalRow(
        1, UTF8String.fromString(label), 1, UTF8String.fromString(lookupLabelValue))
      def matches(condition: Expression, input: InternalRow): Boolean = {
        val interpreted = BindReferences.bindReference(condition, joinedOutput).eval(input)
        val generated = CatalystPredicate.create(condition, joinedOutput).eval(input)
        assert(generated === (interpreted == true))
        generated
      }

      // A hash join never matches a NULL key, so it does not reach the cast. Neither does the
      // residual.
      assert(matches(residual, row("x", null)) === false)
      val error = intercept[Exception](matches(conjunction, row("x", null)))
      // The catalyst DSL turns a String operand of `===` into an expression.
      assert(error.asInstanceOf[SparkThrowable].getCondition == "CAST_INVALID_INPUT")
      // Otherwise the residual matches the rows that the conjunction matches.
      Seq(row("2", "2"), row("-2", "-2"), row("2", "3")).foreach { input =>
        assert(matches(residual, input) === matches(conjunction, input))
      }
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
    assert(reversed.lookupKeyComparisons ===
      Seq(LookupKeyComparison.Equal, LookupKeyComparison.Equal))
    assert(reversed.lookupKeys.map(_.name) === Seq("label", "id"))
    assert(reversed.residualCondition === Some(conjuncts(2) && conjuncts(3)))
    assert(reversed.output === joinOutput)
    assert(PrepareExternalLookupJoin(reversed) === reversed)

    // An unselected equality stays ahead of the conjuncts written after it.
    val (idOnly, _, idOnlyConjuncts) = prepared(candidates => Array(candidates(0)))
    assert(idOnly.lookupParameters === Seq(streamId))
    assert(idOnly.residualCondition === Some(idOnlyConjuncts.drop(1).reduce(_ && _)))
  }

  test("a candidate is offered with the lookup column first, also for a literal value") {
    var offered = Seq.empty[String]
    val lookup = lookupRelation(PlanningExternalLookupTable("id INT, value STRING", select = {
      candidates =>
        offered = candidates.map(_.toString).toSeq
        candidates
    }))
    val Seq(id, value) = lookup.output
    val streamId = stream.output.head
    // A value that reads no column, such as a literal, is a parameter as well.
    val conjuncts = Seq(streamId === id, Literal("a") === value, Literal(0) < id, id <= streamId)
    val prepared = prepare(lookupJoin(lookup, conjuncts.reduce(_ && _)))
    assert(offered === Seq("id = ?", "value = ?", "id > ?", "id <= ?"))
    assert(prepared.lookupParameters === Seq(streamId, Literal("a"), Literal(0), streamId))
    assert(prepared.residualCondition.isEmpty)
  }

  test("a null-safe equality and a range are lookup candidates only on types they are safe on") {
    // `<=>` has the allowlist of `=`. A range is offered only on integral, DECIMAL and datetime
    // types: Spark orders strings by code point and BINARY by unsigned byte, and stores often do
    // not.
    val rangeTypes = Seq("TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL(10, 2)", "DATE",
      "TIMESTAMP", "TIMESTAMP_NTZ")
    val equalityOnlyTypes =
      Seq("BOOLEAN", "BINARY", "STRING", "STRING COLLATE UTF8_BINARY", "VARCHAR(3)")
    val neitherTypes =
      Seq("FLOAT", "DOUBLE", "CHAR(3)", "STRING COLLATE UTF8_LCASE", "STRUCT<a: INT>")
    def offeredFor(codeType: String): Seq[String] = {
      var offered = Seq.empty[String]
      val lookup = lookupRelation(PlanningExternalLookupTable(s"id INT, code $codeType", select = {
        candidates =>
          offered = candidates.map(_.toString).toSeq
          candidates.take(1)
      }))
      val Seq(lookupId, lookupCode) = lookup.output
      val streamId = stream.output.head
      val streamCode = AttributeReference("streamCode", lookupCode.dataType)()
      val conjuncts =
        Seq(streamId === lookupId, streamCode <=> lookupCode, streamCode < lookupCode,
          lookupCode <= streamCode)
      prepare(lookupJoin(lookup, conjuncts.reduce(_ && _),
        left = LocalRelation(Seq(streamId, streamCode), isStreaming = true)))
      offered
    }
    rangeTypes.foreach { codeType =>
      assert(offeredFor(codeType) === Seq("id = ?", "code <=> ?", "code > ?", "code <= ?"),
        codeType)
    }
    equalityOnlyTypes.foreach { codeType =>
      assert(offeredFor(codeType) === Seq("id = ?", "code <=> ?"), codeType)
    }
    neitherTypes.foreach { codeType =>
      assert(offeredFor(codeType) === Seq("id = ?"), codeType)
    }
    // Neither on VARCHAR under standard CHAR/VARCHAR semantics, as `=`.
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      assert(offeredFor("VARCHAR(3)") === Seq("id = ?"))
    }

    // A string or BINARY range alone leaves no candidate.
    Seq("STRING", "BINARY").foreach { codeType =>
      val lookup = lookupRelation(PlanningExternalLookupTable(s"id INT, code $codeType"))
      val lookupCode = lookup.output(1)
      val streamCode = AttributeReference("streamCode", lookupCode.dataType)()
      checkError(
        exception = prepareError(lookupJoin(lookup, lookupCode < streamCode,
          left = LocalRelation(Seq(streamCode), isStreaming = true))),
        condition = "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_CANDIDATE",
        parameters = Map("table" -> QUOTED_TABLE_NAME))
    }
  }

  test("a range is a lookup candidate only when its parameter cannot fail") {
    // Spark evaluates the parameters on each stream row that passes the stream condition, but a
    // join evaluates a range only after its keys match. So a range is offered only when
    // ExprUtils.canEvaluateUnconditionally takes its parameter as unable to fail, cast or not, in
    // the cast's own eval mode. An equality with a parameter that can fail is still offered, as a
    // hash join evaluates its keys on every row too.
    var offered = Seq.empty[String]
    val lookup = lookupRelation(PlanningExternalLookupTable(
      "id INT, value INT, total BIGINT, day DATE, localTime TIMESTAMP_NTZ, time TIMESTAMP, " +
        "price DECIMAL(12, 2), rate DECIMAL(38, 10)",
      select = { candidates =>
        offered = candidates.map(_.toString).toSeq
        candidates.take(1)
      }))
    val Seq(lookupId, lookupValue, lookupTotal, lookupDay, lookupLocalTime, lookupTime, lookupPrice,
      lookupRate) = lookup.output
    val Seq(streamId, streamLabel) = stream.output
    val (streamDay, streamLocalTime, streamTime) =
      ($"streamDay".date, $"streamLocalTime".timestampNTZ, $"streamTime".timestamp)
    val (streamSmall, streamAmount, streamBid, streamUnits) = ($"streamSmall".decimal(18, 0),
      $"streamAmount".decimal(19, 0), $"streamBid".decimal(10, 2), $"streamUnits".decimal(38, 0))
    val left = LocalRelation(Seq(streamId, streamLabel, streamDay, streamLocalTime, streamTime,
      streamSmall, streamAmount, streamBid, streamUnits), isStreaming = true)
    // Prepares the join of an equality on `id`, which is selected, and of the ranges, and checks
    // which ranges are offered, and that the rest of the condition stays in the residual.
    def check(offeredRanges: Seq[(Expression, String)], keptRanges: Seq[Expression]): Unit = {
      val labelNumber = streamLabel.cast(IntegerType)
      val conjuncts = (lookupId === labelNumber) +: (offeredRanges.map(_._1) ++ keptRanges)
      val prepared = prepare(lookupJoin(lookup, conjuncts.reduce(_ && _), left = left))
      assert(offered === "id = ?" +: offeredRanges.map(_._2))
      assert(prepared.lookupParameters === Seq(labelNumber))
      // Which conjuncts go first under a NULL guard depends on what
      // ExprUtils.canEvaluateUnconditionally takes as unable to fail, which another test covers.
      val residual = prepared.residualCondition.toSeq.flatMap(splitConjunctivePredicates).map {
        case Coalesce(Seq(conjunct, Literal(false, BooleanType))) => conjunct
        case conjunct => conjunct
      }
      assert(residual.length === conjuncts.length - 1)
      assert(residual.toSet === conjuncts.drop(1).toSet)
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      check(
        offeredRanges = Seq(
          (lookupValue >= streamId, "value >= ?"),
          // The widening numeric casts that analysis adds, and DECIMAL(18, 0), which BIGINT holds.
          (lookupTotal >= streamId.cast(LongType), "total >= ?"),
          (lookupPrice >= streamId.cast(DecimalType(12, 2)), "price >= ?"),
          (lookupPrice <= streamBid.cast(DecimalType(12, 2)), "price <= ?"),
          (lookupTotal >= streamSmall.cast(LongType), "total >= ?"),
          // The date of a timestamp, and a cast that returns NULL where it would fail.
          (lookupDay >= Cast(streamTime, DateType, Some("UTC")), "day >= ?"),
          (lookupDay <= Cast(streamLocalTime, DateType), "day <= ?"),
          (lookupValue >= Cast(streamLabel, IntegerType, evalMode = EvalMode.TRY), "value >= ?")),
        // Each fails on some values: a string that is not a number, a far-future DATE or a
        // TIMESTAMP_NTZ near the limits as a timestamp, DECIMAL(19, 0) beyond BIGINT, and
        // DECIMAL(38, 0) as DECIMAL(38, 10), which has room for only 28 integer digits.
        keptRanges = Seq(
          lookupValue < streamLabel.cast(IntegerType),
          lookupLocalTime >= Cast(streamDay, TimestampNTZType),
          lookupTime >= Cast(streamDay, TimestampType, Some("UTC")),
          lookupTime >= Cast(streamLocalTime, TimestampType, Some("UTC")),
          lookupTotal >= streamAmount.cast(LongType),
          lookupRate >= streamUnits.cast(DecimalType(38, 10))))
    }
    // With ANSI off, a string that is not a number becomes NULL, but a far-future DATE still
    // overflows as a timestamp.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      check(
        offeredRanges = Seq((lookupValue < streamLabel.cast(IntegerType), "value < ?")),
        keptRanges = Seq(lookupTime >= Cast(streamDay, TimestampType, Some("UTC"))))
    }
  }

  test("the read schema binds to the analyzed lookup attributes by exact name") {
    val lookup = lookupRelation(PlanningExternalLookupTable(
      "id INT NOT NULL, value VARCHAR(8) NOT NULL, s STRUCT<a: INT NOT NULL>",
      readSchemaDDL = Some("s STRUCT<a: INT>, value VARCHAR(8), id INT NOT NULL")))
    val join = lookupJoinOnId(lookup)
    val prepared = prepare(join)
    assert(prepared.lookupOutput === lookup.output)
    assert(prepared.output === join.output)
    // The value-row attributes follow the read schema, with its types. A column or nested field
    // is nullable only when both the table and the read schema allow nulls: the join fails the
    // task on a null that either rules out, so `value` and `s.a` stay NOT NULL.
    val relationAttrs = lookup.output.map(attr => attr.name -> attr).toMap
    assert(prepared.lookupInput.map(_.name) === Seq("s", "value", "id"))
    assert(prepared.lookupInput.map(_.nullable) === Seq(true, false, false))
    assert(prepared.lookupInput.head.dataType === StructType.fromDDL("a INT NOT NULL"))
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

  test("nullability merges at every nested level, in arrays and maps too") {
    // The table rules out nulls in `tags` elements and `scores` values, and the read schema rules
    // them out in `labels` elements. Each value-row attribute keeps the stricter of the two.
    def schemaWith(tagsNull: Boolean, scoresNull: Boolean, labelsNull: Boolean): StructType =
      new StructType()
        .add("id", IntegerType)
        .add("tags", ArrayType(IntegerType, tagsNull))
        .add("scores", MapType(StringType, IntegerType, scoresNull))
        .add("labels", ArrayType(StringType, labelsNull))
    val lookup = lookupRelation(PlanningExternalLookupTable(
      tableSchema = Some(schemaWith(tagsNull = false, scoresNull = false, labelsNull = true)),
      lookupReadSchema = Some(schemaWith(tagsNull = true, scoresNull = true, labelsNull = false))))
    val prepared = prepare(lookupJoinOnId(lookup))
    assert(prepared.lookupInput.map(_.dataType) ===
      schemaWith(tagsNull = false, scoresNull = false, labelsNull = false).map(_.dataType))
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

    Seq[(Array[Predicate] => Array[Predicate], String, Map[String, String])](
      (_ => Array.empty, "EXTERNAL_LOOKUP_JOIN_UNSUPPORTED.NO_LOOKUP_PREDICATE", Map.empty),
      (_ => null, "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_RESULT",
        Map("method" -> "SupportsExternalLookup.selectLookupPredicates()")),
      // A null element is not one of the offered instances.
      (_ :+ null, "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.UNOFFERED_SELECTED_PREDICATE",
        Map("predicate" -> "null"))).foreach {
      case (select, condition, parameters) =>
        checkError(
          exception = prepareError(lookupJoinOnId(
            lookupRelation(PlanningExternalLookupTable(select = select)))),
          condition = condition,
          parameters = Map("table" -> QUOTED_TABLE_NAME) ++ parameters)
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

  test("a prepared lookup join shows its join type, parameters and residual in its plan string") {
    val lookup = lookupRelation(PlanningExternalLookupTable())
    val Seq(streamId, streamLabel) = stream.output
    val Seq(id, value) = lookup.output
    val residual = streamLabel === value
    val prepared = prepare(lookupJoin(lookup, streamId === id && residual, LeftOuter))
    Seq(prepared.toString, prepared.simpleString(100)).foreach { rendered =>
      Seq(LeftOuter.toString, streamId.toString, residual.toString).foreach { arg =>
        assert(rendered.contains(arg), rendered)
      }
    }
  }

  test("a connector method that returns null fails with a named error") {
    Seq(
      "build" -> "ScanBuilder.build()",
      "readSchema" -> "Scan.readSchema()",
      "toExternalLookup" -> "Scan.toExternalLookup()",
      "createReaderFactory" -> "ExternalLookup.createReaderFactory()").foreach {
      case (nullFrom, method) =>
        val lookup = lookupRelation(PlanningExternalLookupTable(nullFrom = nullFrom))
        checkError(
          exception = prepareError(lookupJoinOnId(lookup)),
          condition = "DATA_SOURCE_INVALID_EXTERNAL_LOOKUP.NULL_RESULT",
          parameters = Map("table" -> QUOTED_TABLE_NAME, "method" -> method))
    }
  }

  test("a prepared lookup join reports the stream side's size times Spark's default size") {
    // Two stream rows, and a lookup side of Spark's default size. The lookup side is the table, not
    // a projection of it, so the estimate is also the one before preparation.
    withSQLConf(SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "1000") {
      val rows = Seq(1, 2).map(id => InternalRow(id, UTF8String.fromString("a")))
      val sizedStream = LocalRelation(stream.output, rows, isStreaming = true)
      assert(sizedStream.stats.sizeInBytes > 0)
      // Inner and left outer joins get the product of both sides' sizes. Semi and anti joins, as
      // ordinary ones, get the stream side's size.
      Seq(Inner -> 1000, LeftOuter -> 1000, LeftSemi -> 1, LeftAnti -> 1).foreach {
        case (joinType, lookupFactor) =>
          val lookup = lookupRelation(PlanningExternalLookupTable())
          val join = lookupJoin(lookup, sizedStream.output.head === lookup.output.head, joinType,
            left = sizedStream)
          val prepared = prepare(join)
          assert(prepared.stats.sizeInBytes === sizedStream.stats.sizeInBytes * lookupFactor,
            joinType)
          assert(prepared.stats.sizeInBytes === join.stats.sizeInBytes, joinType)
      }
    }
  }

  test("a prepared lookup join canonicalizes each lookup column by its place in the read schema") {
    val lookup = lookupRelation(PlanningExternalLookupTable("id INT, code INT"))
    val Seq(lookupId, lookupCode) = lookup.output
    val streamId = stream.output.head
    val prepared = prepare(lookupJoin(lookup, streamId === lookupId && streamId > lookupCode))
    assert(prepared.residualCondition.exists(_.references.contains(lookupCode)))
    // Another analysis of the query gives the lookup columns other ids.
    val newIds = AttributeMap(prepared.lookupInput.map(attr => attr -> attr.newInstance()))
    val reanalyzed = prepared.transformExpressions {
      case attr: Attribute if newIds.contains(attr) => newIds(attr)
    }
    assert(reanalyzed.lookupInput != prepared.lookupInput)
    assert(reanalyzed.sameResult(prepared))
    // Joins that keep different lookup columns of one type do not canonicalize alike.
    val keepsId = prepared.copy(lookupOutput = Seq(lookupId))
    val keepsCode = prepared.copy(lookupOutput = Seq(lookupCode))
    assert(keepsId.sameResult(keepsCode) === false)
    assert(keepsId.sameResult(prepared.copy(lookupOutput = Seq(lookupId))))
  }

  test("prepared lookup joins with different lookup-side stages canonicalize apart") {
    // A projection stage that computes a team from a STRING column, and a filter stage on it, over
    // a table of two STRING columns.
    val lookup =
      lookupRelation(PlanningExternalLookupTable("id INT, department STRING, name STRING"))
    val Seq(id, department, name) = lookup.output
    val team = Alias(Upper(department), "team")()
    val staged = Filter(Not(EqualTo(team.toAttribute, Literal("SALES"))),
      Project(lookup.output :+ team, lookup))
    val prepared = prepare(lookupJoin(staged, stream.output.head === id))
    assert(prepared.lookupTransforms.length === 2)
    // The same join after another analysis, which gives every lookup-side attribute another id.
    val newIds = AttributeMap(ExternalLookupTransform.attributes(prepared.lookupInput,
      prepared.lookupTransforms).map(attr => attr -> attr.newInstance()))
    def renew[T <: Expression](expression: T): T = expression.transformUp {
      case alias: Alias if newIds.contains(alias.toAttribute) =>
        Alias(alias.child, alias.name)(newIds(alias.toAttribute).exprId)
      case attr: Attribute if newIds.contains(attr) => newIds(attr)
    }.asInstanceOf[T]
    val reanalyzed = prepared.transformExpressions {
      case attr: Attribute if newIds.contains(attr) => newIds(attr)
    }.asInstanceOf[PreparedExternalLookupJoin].copy(lookupTransforms =
      prepared.lookupTransforms.map {
        case ExternalLookupProject(projectList, input) =>
          ExternalLookupProject(projectList.map(renew), input.map(renew))
        case ExternalLookupFilter(condition, input) =>
          ExternalLookupFilter(renew(condition), input.map(renew))
      })
    assert(reanalyzed.lookupInput != prepared.lookupInput)
    assert(reanalyzed.sameResult(prepared))
    // A stage expression over the other column of the same type.
    val byName = prepared.copy(lookupTransforms = prepared.lookupTransforms.map {
      case project: ExternalLookupProject => project.copy(projectList =
        project.projectList.map(_.transform { case Upper(_) => Upper(name) }
          .asInstanceOf[NamedExpression]))
      case other => other
    })
    assert(byName.sameResult(prepared) === false)
    // Joins that keep different columns of one type, of the read schema or of a stage.
    val stagedTeam = prepared.lookupTransforms.collectFirst {
      case project: ExternalLookupProject => project.output.last
    }.get
    Seq(Seq(department), Seq(name), Seq(stagedTeam)).combinations(2).foreach {
      case Seq(one, other) =>
        assert(prepared.copy(lookupOutput = one)
          .sameResult(prepared.copy(lookupOutput = other)) === false, s"$one, $other")
    }
  }

  test("an external UDF over lookup columns in the join condition") {
    val lookup = lookupRelation(PlanningExternalLookupTable())
    val lookupId = lookup.output.head
    val lookupUDF = ExternalUserDefinedFunction(
      name = Some("lookup_udf"),
      workerSpec = UDFWorkerSpecification.getDefaultInstance,
      payload = Array.emptyByteArray,
      dataType = BooleanType,
      children = Seq(lookupId),
      udfDeterministic = true,
      udfNullable = false)
    val condition = stream.output.head === lookupId && lookupUDF

    // An inner join evaluates it above the lookup join, where PlanExternalUDFs extracts it.
    val prepared = PrepareExternalLookupJoin(lookupJoin(lookup, condition))
    prepared match {
      case Filter(filterCondition, join: PreparedExternalLookupJoin) =>
        assert(filterCondition == lookupUDF)
        assert(join.residualCondition.isEmpty)
      case other => fail(s"Expected a filter above a prepared external lookup join: $other")
    }
    val conf = SQLConf.get
    val unifiedUDFExecution = conf.getConf(StaticSQLConf.UNIFIED_UDF_EXECUTION_ENABLED)
    conf.setConf(StaticSQLConf.UNIFIED_UDF_EXECUTION_ENABLED, true)
    try {
      val planned = PlanExternalUDFs(prepared)
      assert(planned.exists(_.isInstanceOf[ExecuteExternalUDF]))
      assert(planned.exists {
        case _: ExecuteExternalUDF => false
        case node => node.expressions.exists(_.exists(_.isInstanceOf[ExternalUserDefinedFunction]))
      } === false)
    } finally {
      conf.setConf(StaticSQLConf.UNIFIED_UDF_EXECUTION_ENABLED, unifiedUDFExecution)
    }
    // A left outer join cannot.
    checkError(
      exception = prepareError(lookupJoin(lookup, condition, LeftOuter)),
      condition = "UNSUPPORTED_FEATURE.EXTERNAL_UDF_IN_ON_CLAUSE",
      parameters = Map("joinType" -> "LEFT OUTER"))
  }

  test("column pruning narrows the lookup columns of a lookup join before projecting them") {
    val lookup = lookupRelation(PlanningExternalLookupTable())
    val pruned = ColumnPruning(Project(Seq(stream.output.head), lookupJoinOnId(lookup)))
    val (lookupLeft, lookupRight) =
      pruned.collectFirst { case join: ExternalLookupJoin => (join.left, join.right) }.get
    assert(lookupLeft.output === Seq(stream.output.head))
    // One pass narrows the lookup columns that the join keeps; only a later one projects them.
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
    nullFrom: String = "",
    // For nullability that DDL cannot write, such as of array elements.
    tableSchema: Option[StructType] = None,
    lookupReadSchema: Option[StructType] = None)
  extends Table with SupportsRead {
  override def name(): String = "PlanningExternalLookupTable"
  override def schema(): StructType = tableSchema.getOrElse(StructType.fromDDL(schemaDDL))
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(EXTERNAL_LOOKUP)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SupportsExternalLookup {
      override def selectLookupPredicates(candidates: Array[Predicate]): Array[Predicate] =
        select(candidates)
      override def build(): Scan = if (nullFrom == "build") null else new Scan {
        override def readSchema(): StructType = if (nullFrom == "readSchema") null else {
          lookupReadSchema.orElse(readSchemaDDL.map(StructType.fromDDL)).getOrElse(schema())
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
