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

package org.apache.spark.sql.catalyst.util

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime, Period}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal => CatalystLiteral}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryTableCatalog, TableInfo, WriteDistributionMode}
import org.apache.spark.sql.connector.expressions.{Cast, Expression, Expressions, Extract, FieldReference, GeneralScalarExpression, GetArrayItem, IdentityTransform, NamedReference, NullOrdering, SortDirection, SortOrder, Transform, UserDefinedScalarFunc, VariantGet}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.{apply => transform, literal, sort}
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class WriteDistributionAndOrderingUtilsSuite extends SparkFunSuite with SQLHelper {
  import WriteDistributionAndOrdering._

  private class ConnectorReference(names: String*) extends NamedReference {
    override def fieldNames(): Array[String] = names.toArray
  }

  // A connector's own expression, which Spark renders with its `describe`.
  private class ConnectorFunction(rendered: String, args: Expression*) extends Expression {
    override def children(): Array[Expression] = args.toArray
    override def describe(): String = rendered
    override def toString: String = rendered
  }

  // A key whose arguments and `describe` both throw `error`.
  private class UnrenderableTransform(
      error: () => Throwable,
      calls: AtomicInteger = new AtomicInteger(0)) extends Transform {
    override def name(): String = "unrenderable"
    override def references(): Array[NamedReference] = Array.empty
    override def arguments(): Array[Expression] = {
      calls.incrementAndGet()
      throw error()
    }
    override def toString: String = s"unrenderable(${arguments().mkString(", ")})"
  }

  private val id = FieldReference("id")

  private def f(arg: Expression): Transform = transform("f", id, arg)

  private def key(e: Expression): SortOrder =
    sort(e, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)

  // Runs `body` on a thread with a small (256 KiB) stack so that a recursive rendering overflows
  // deterministically, and returns whatever it threw (or null if nothing did). The wait is
  // bounded because SparkFunSuite's per-test timeout cannot interrupt a thread blocked in `join`,
  // so a rendering that never finishes would otherwise hang the whole suite. Such a thread keeps
  // running after the test fails.
  private def runOnSmallStack(body: => Unit): Throwable = {
    @volatile var thrown: Throwable = null
    val runnable = new Runnable {
      override def run(): Unit = {
        try body catch { case t: Throwable => thrown = t }
      }
    }
    val t = new Thread(null, runnable, "write-ordering-deep-key", 256 * 1024)
    t.setDaemon(true)
    t.start()
    t.join(TimeUnit.MINUTES.toMillis(1))
    assert(!t.isAlive, "the rendering did not finish")
    thrown
  }

  private val schema = StructType.fromDDL(
    "id INT, `order` INT, `order-id` INT, s STRUCT<x: INT>, a ARRAY<STRUCT<x: INT>>")

  private val replay = replayWith(CatalystSqlParser) _

  private def emitted(e: Expression): Option[String] = {
    writeClausesSQL(WriteDistributionMode.RANGE, Seq(key(e)), Seq.empty, schema, replay)
  }

  private val spellable: Seq[(String, Expression)] = Seq(
    "reference" -> id,
    "nested field" -> FieldReference(Seq("s", "x")),
    "connector reference" -> transform("f", new ConnectorReference("order-id")),
    "identity" -> IdentityTransform(id),
    "identity by name" -> transform("identity", id),
    "byte" -> f(literal(1.toByte)),
    "short" -> f(literal(1.toShort)),
    "int" -> f(literal(1)),
    "long" -> f(literal(1L)),
    "float" -> f(literal(1.5F)),
    "double" -> f(literal(1.5D)),
    "decimal" -> f(literal(new BigDecimal("1.5"))),
    "string" -> f(literal("x")),
    "string with escapes" -> f(literal("it's a \\")),
    "binary" -> f(literal(Array[Byte](1, 2))),
    "date" -> f(literal(LocalDate.of(1970, 1, 1))),
    "timestamp" -> f(literal(Instant.parse("2020-11-01T09:30:00Z"))),
    "timestamp_ntz" -> f(literal(LocalDateTime.of(2020, 1, 1, 10, 0))),
    "time" -> f(literal(LocalTime.of(12, 0))),
    "time(7) with trailing zeros" -> f(literal(LocalTime.of(12, 0, 0, 100000000), TimeType(7))),
    "time(9) with trailing zeros" -> f(literal(LocalTime.of(12, 0, 0, 100000000), TimeType(9))),
    "time(9) without trailing zeros" ->
      f(literal(LocalTime.of(12, 0, 0, 123456789), TimeType(9))),
    "time(7) at a whole second" -> f(literal(LocalTime.of(12, 0), TimeType(7))),
    "day-time interval" -> f(literal(Duration.ofHours(1))),
    "year-month interval" -> f(literal(Period.ofMonths(14))),
    "bucket" -> transform("bucket", literal(16), id),
    "bucket without columns" -> transform("bucket", literal(16)),
    "Spark's bucket" -> Expressions.bucket(16, "id"),
    "days" -> transform("days", id),
    "unparsed name" -> transform("Days", id, literal("UTC")))

  private val unspellable: Seq[(String, Expression)] = Seq(
    "nested transform" -> transform("f", transform("g", id)),
    "nested identity" -> transform("f", IdentityTransform(id)),
    "true" -> f(literal(true)),
    "false" -> f(literal(false)),
    "null" -> f(literal(null)),
    "typed null" -> f(literal(null, IntegerType)),
    "decimal with trailing zeros" ->
      f(literal(new BigDecimal("1.50"), DecimalType(10, 2))),
    "decimal in exponent form" ->
      f(literal(new BigDecimal("0.0000001"), DecimalType(10, 7))),
    "char" -> f(literal("x", CharType(3))),
    "varchar" -> f(literal("x", VarcharType(3))),
    "collated string" -> f(literal("x", StringType("UTF8_LCASE"))),
    "NaN" -> f(literal(Float.NaN)),
    "infinity" -> f(literal(Double.PositiveInfinity)),
    "maximal float" -> f(literal(Float.MaxValue)),
    "time(6) holding a nanosecond" -> f(literal(LocalTime.ofNanoOfDay(1))),
    "year interval not in whole years" ->
      f(literal(13, YearMonthIntervalType(YearMonthIntervalType.YEAR))),
    "hour interval not in whole hours" ->
      f(literal(Duration.ofMinutes(90).toNanos / 1000, DayTimeIntervalType(
        DayTimeIntervalType.HOUR))),
    "negative-scale decimal" -> f(Expressions.literal(new BigDecimal("1E+3"))),
    "int value with long type" -> f(literal(5, LongType)),
    "byte buffer value" -> f(literal(ByteBuffer.wrap(Array[Byte](1, 2)), BinaryType)),
    "bucket with columns first" -> transform("bucket", id, literal(16)),
    "bucket with long count" -> transform("bucket", literal(16L), id),
    "bucket with literal column" -> transform("bucket", literal(16), literal(1)),
    "days with two arguments" -> transform("days", id, literal("UTC")),
    "hours of a literal" -> transform("hours", literal(1)),
    "missing column" -> FieldReference("missing"),
    "missing column inside a nested identity" ->
      transform("f", Expressions.identity("missing")),
    "column in a different case" -> FieldReference("ID"),
    "field of an array element" -> FieldReference(Seq("a", "x")),
    "reference with no field names" -> new ConnectorReference())

  test("a sort key is emitted iff its rendering replays as the same key") {
    assert(spellable.filter { case (_, e) =>
      !emitted(e).contains(s"ORDERED BY (${describeSortOrder(key(e))})")
    }.map(_._1) === Seq.empty)
    assert(unspellable.filter { case (_, e) => emitted(e).isDefined }.map(_._1) === Seq.empty)
  }

  test("a sort key is emitted iff it replays as the same key under parser confs") {
    val timestamp = f(literal(Instant.parse("2020-11-01T09:30:00Z")))
    val wrong = Seq(
      (SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles", timestamp, true),
      (SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_NTZ", timestamp, true),
      (SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true", f(literal(Duration.ofHours(1))), false),
      (SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true", f(literal(Period.ofMonths(14))), false),
      (SQLConf.ESCAPED_STRING_LITERALS.key -> "true", f(literal("it's")), false),
      (SQLConf.ESCAPED_STRING_LITERALS.key -> "true", f(literal("a\\b")), false),
      (SQLConf.ESCAPED_STRING_LITERALS.key -> "true", f(literal("x")), true),
      (SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY",
        f(literal(LocalDate.of(-1, 1, 1))), false),
      (SQLConf.LEGACY_FROM_DAYTIME_STRING.key -> "true",
        f(literal(Duration.ofHours(26), DayTimeIntervalType(
          DayTimeIntervalType.DAY, DayTimeIntervalType.HOUR))), false)
    ).filter { case (conf, e, spelled) => withSQLConf(conf)(emitted(e).isDefined != spelled) }
    assert(wrong.map(_._2.describe) === Seq.empty)
  }

  test("a name that is a reserved keyword is emitted quoted only where the parser needs it") {
    val order = FieldReference("order")
    assert(emitted(order) === Some("ORDERED BY (order ASC NULLS FIRST)"))
    withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
      assert(emitted(order) === Some("ORDERED BY (`order` ASC NULLS FIRST)"))
      assert(emitted(transform("bucket", literal(4), order)) ===
        Some("ORDERED BY (`bucket`(4, `order`) ASC NULLS FIRST)"))
    }
  }

  test("each mode with a clause form is emitted as that clause") {
    val keys = Seq(key(id))
    val partitioning = Seq(Expressions.identity("p"))
    Seq(
      (WriteDistributionMode.HASH, keys,
        "DISTRIBUTED BY PARTITION ORDERED BY (id ASC NULLS FIRST)"),
      (WriteDistributionMode.HASH, Seq.empty, "DISTRIBUTED BY PARTITION"),
      (WriteDistributionMode.RANGE, keys, "ORDERED BY (id ASC NULLS FIRST)"),
      (WriteDistributionMode.NONE, keys, "LOCALLY ORDERED BY (id ASC NULLS FIRST)"),
      (WriteDistributionMode.NONE, Seq.empty, "UNORDERED")
    ).foreach { case (mode, ordering, clauses) =>
      assert(writeClausesSQL(mode, ordering, partitioning, schema, replay) === Some(clauses),
        s"for $mode $ordering")
    }
  }

  test("a key that does not parse back omits the pair in every mode") {
    val keys = Seq(key(transform("f", transform("g", id))))
    Seq(WriteDistributionMode.HASH, WriteDistributionMode.RANGE, WriteDistributionMode.NONE)
      .foreach { mode =>
        assert(writeClausesSQL(mode, keys, Seq(Expressions.identity("p")), schema, replay).isEmpty,
          s"for $mode")
      }
  }

  test("the pairs with no clause form are not emitted") {
    val keys = Seq(key(id))
    Seq(
      (WriteDistributionMode.HASH, keys, Seq.empty[Transform]),
      (WriteDistributionMode.HASH, Seq.empty, Seq(Expressions.apply("cluster_by", id))),
      (WriteDistributionMode.RANGE, Seq.empty, Seq.empty[Transform]),
      (null, keys, Seq.empty[Transform]),
      (null, Seq.empty, Seq.empty[Transform])
    ).foreach { case (mode, ordering, partitioning) =>
      val anyClauses = (_: String) => Some((mode, ordering))
      assert(writeClausesSQL(mode, ordering, partitioning, schema, anyClauses).isEmpty,
        s"for $mode")
    }
  }

  test("a pair with no clause form is decided before anything is rendered") {
    val calls = new AtomicInteger(0)
    val keys = Seq(key(new UnrenderableTransform(() => new IllegalStateException(), calls)))
    assert(writeClausesSQL(WriteDistributionMode.HASH, keys, Seq.empty, schema, replay).isEmpty)
    assert(calls.get === 0)
  }

  test("a clause that quoting cannot change is replayed once") {
    val replays = new AtomicInteger(0)
    val countingReplay = (_: String) => {
      replays.incrementAndGet()
      None
    }
    assert(
      writeClausesSQL(WriteDistributionMode.NONE, Seq.empty, Seq.empty, schema, countingReplay)
        .isEmpty)
    assert(replays.get === 1)
  }

  test("rejectRequested rejects a request with a mode or an ordering and nothing else") {
    val catalog = new InMemoryTableCatalog
    catalog.initialize("cat", CaseInsensitiveStringMap.empty())
    val ident = Identifier.of(Array("ns"), "t")
    def info(
        mode: WriteDistributionMode,
        ordering: Array[SortOrder]): TableInfo = {
      new TableInfo.Builder()
        .withColumns(Array(Column.create("id", IntegerType)))
        .withWriteDistributionMode(mode)
        .withWriteOrdering(ordering)
        .build()
    }

    rejectRequested(catalog, ident, info(null, Array.empty))
    Seq(
      info(WriteDistributionMode.NONE, Array.empty),
      info(null, Array(key(id)))
    ).foreach { requested =>
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          rejectRequested(catalog, ident, requested)
        },
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        sqlState = "0A000",
        parameters = Map("tableName" -> "`cat`.`ns`.`t`", "operation" -> CLAUSES))
    }
  }

  test("a rendering that fails is not tried again with other quoting") {
    val calls = new AtomicInteger(0)
    val keys = Seq(key(new UnrenderableTransform(() => new IllegalStateException(), calls)))
    assert(writeClausesSQL(WriteDistributionMode.RANGE, keys, Seq.empty, schema, replay).isEmpty)
    assert(calls.get === 1)
  }

  test("a nanosecond TIMESTAMP_LTZ renders in UTC with an explicit offset") {
    withSQLConf(
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val parsed = CatalystSqlParser.parseExpression(
        "TIMESTAMP_LTZ '2020-11-01 01:30:00.123456789-08:00'").asInstanceOf[CatalystLiteral]
      val e = f(literal(parsed.value, parsed.dataType))
      assert(emitted(e) ===
        Some("ORDERED BY (f(id, TIMESTAMP_LTZ '2020-11-01 09:30:00.123456789Z') ASC NULLS FIRST)"))

      val wholeSecond = CatalystSqlParser.parseExpression(
        "TIMESTAMP_LTZ '2020-11-01 01:30:00.000000000-08:00'").asInstanceOf[CatalystLiteral]
      assert(emitted(f(literal(wholeSecond.value, wholeSecond.dataType))) ===
        Some("ORDERED BY (f(id, TIMESTAMP_LTZ '2020-11-01 09:30:00.000000000Z') ASC NULLS FIRST)"))

      val ntz = CatalystSqlParser.parseExpression("TIMESTAMP_NTZ '2020-01-01 10:00:00.100000000'")
        .asInstanceOf[CatalystLiteral]
      assert(emitted(f(literal(ntz.value, ntz.dataType))).isDefined)
    }
  }

  test("a timestamp renders in UTC with an explicit offset") {
    val e = f(literal(Instant.parse("2020-11-01T09:30:00Z")))
    Seq("UTC", "America/Los_Angeles", "Asia/Tokyo").foreach { tz =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
        assert(describeSortOrder(key(e)) ===
          "f(id, TIMESTAMP_LTZ '2020-11-01 09:30:00Z') ASC NULLS FIRST", s"in $tz")
      }
    }
  }

  test("DESCRIBE renders a literal Catalyst cannot represent with describe") {
    Seq(
      Expressions.literal(new BigDecimal("1E+3")) -> "1E+3",
      literal(5, LongType) -> "5"
    ).foreach { case (lit, rendered) =>
      assert(describeSortOrder(key(f(lit))) === s"f(id, $rendered) ASC NULLS FIRST")
    }
    val buffer = ByteBuffer.wrap(Array[Byte](1, 2))
    assert(describeSortOrder(key(f(literal(buffer, BinaryType)))) ===
      s"f(id, $buffer) ASC NULLS FIRST")
  }

  test("DESCRIBE renders a connector expression in its SQL form") {
    Seq(
      new Extract("YEAR", id) -> "EXTRACT(YEAR FROM id)",
      new UserDefinedScalarFunc("my_udf", "my_udf", Array[Expression](id)) -> "my_udf(id)",
      new GeneralScalarExpression("+", Array[Expression](id, literal(1))) -> "id + 1",
      new GeneralScalarExpression("+", Array[Expression](IdentityTransform(id), literal(1))) ->
        "identity(id) + 1",
      new Cast(Expressions.bucket(16, "id"), StringType) -> "CAST(bucket(16, id) AS STRING)",
      new Cast(id, ArrayType(IntegerType)) -> "CAST(id AS ARRAY<INT>)",
      new UserDefinedScalarFunc("zorder_key", "zorder_key", Array[Expression](
        transform("zorder", id, FieldReference("b")))) -> "zorder_key(zorder(id, b))",
      new VariantGet(id, null, IntegerType, true, null) -> "variant_get(id, 'null', int)"
    ).foreach { case (e, rendered) =>
      assert(describeSortOrder(key(transform("f", e))) === s"f($rendered) ASC NULLS FIRST")
    }
  }

  test("DESCRIBE renders a connector's own expression under a Spark expression with its describe") {
    val truncate = new ConnectorFunction("truncate(4, id)", id, literal(4))
    Seq(
      new GeneralScalarExpression("ABS", Array[Expression](truncate)) -> "ABS(truncate(4, id))",
      new GeneralScalarExpression("+", Array[Expression](truncate, literal(1))) ->
        "(truncate(4, id)) + 1",
      new Cast(truncate, IntegerType) -> "CAST(truncate(4, id) AS INT)",
      new Extract("YEAR", truncate) -> "EXTRACT(YEAR FROM truncate(4, id))",
      new GetArrayItem(truncate, literal(0), true) -> "truncate(4, id)[0]",
      truncate -> "truncate(4, id)"
    ).foreach { case (e, rendered) =>
      assert(describeSortOrder(key(transform("f", e))) === s"f($rendered) ASC NULLS FIRST")
      assert(describeSortOrder(key(e)) === s"$rendered ASC NULLS FIRST")
    }
  }

  test("DESCRIBE renders a sort order that cannot be rendered with the class of its key") {
    Seq[() => Throwable](
      () => new IllegalStateException("broken"),
      () => new NoClassDefFoundError("org/example/MissingClass"),
      () => new StackOverflowError()
    ).foreach { error =>
      val unrenderable = new UnrenderableTransform(error)
      assert(describeSortOrder(key(unrenderable)) ===
        s"${classOf[UnrenderableTransform].getName} ASC NULLS FIRST", error().getClass.getName)
      assert(emitted(unrenderable).isEmpty, error().getClass.getName)
    }
  }

  test("a key that fails with a StackOverflowError costs the same at any depth") {
    def callsFor(depth: Int): Int = {
      val calls = new AtomicInteger(0)
      val leaf = new UnrenderableTransform(() => new StackOverflowError(), calls)
      val nested = (1 to depth).foldLeft[Expression](leaf) { (child, _) =>
        new GeneralScalarExpression("ABS", Array[Expression](child))
      }
      assert(describeSortOrder(key(nested)) ===
        s"${classOf[GeneralScalarExpression].getName} ASC NULLS FIRST")
      assert(emitted(nested).isEmpty)
      calls.get()
    }

    assert(callsFor(10) === callsFor(1))
  }

  // Without the entry-point handling this test fails only after the bounded wait, so the test
  // above is the one that fails at once. This one checks the result for a key that really
  // overflows the stack.
  test("a key that contains itself overflows the stack and is described by its class") {
    val children = new Array[Expression](1)
    val selfContaining = new GeneralScalarExpression("ABS", children)
    children(0) = selfContaining

    val thrown = runOnSmallStack {
      assert(describeSortOrder(key(selfContaining)) ===
        s"${classOf[GeneralScalarExpression].getName} ASC NULLS FIRST")
      assert(emitted(selfContaining).isEmpty)
    }
    assert(thrown == null, s"expected no error but got: $thrown")
  }

  test("DESCRIBE renders a null sort order as null") {
    assert(describeSortOrder(null) === "null")
  }

  test("a replay that fails with an error is not emitted, however it fails") {
    Seq[() => Throwable](
      () => new IllegalStateException("broken"),
      () => new NoClassDefFoundError("org/example/MissingClass"),
      () => new StackOverflowError()
    ).foreach { error =>
      val failing = (_: String) => throw error()
      assert(writeClausesSQL(
        WriteDistributionMode.RANGE, Seq(key(id)), Seq.empty, schema, failing).isEmpty,
        error().getClass.getName)
    }
  }

  test("DESCRIBE renders a connector expression describe cannot build from its children") {
    val truncated = new GeneralScalarExpression("DATE_TRUNC", Array[Expression](id))
    Seq(
      truncated -> "DATE_TRUNC(id)",
      new UserDefinedScalarFunc("my_udf", "my_udf", Array[Expression](truncated)) ->
        "my_udf(DATE_TRUNC(id))",
      new Cast(truncated, IntegerType) -> "CAST(DATE_TRUNC(id) AS INT)",
      new GetArrayItem(truncated, literal(0), true) -> "DATE_TRUNC(id)[0]",
      new VariantGet(truncated, "$.a", IntegerType, true, null) ->
        "variant_get(DATE_TRUNC(id), '$.a', int)",
      sort(truncated, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST) ->
        "DATE_TRUNC(id) ASC NULLS FIRST",
      new GeneralScalarExpression("+", Array[Expression](id, null)) -> "+(id, null)",
      new UserDefinedScalarFunc("my_udf", "my_udf", Array[Expression](
        new GeneralScalarExpression("+", Array[Expression](id, null)))) -> "my_udf(+(id, null))",
      new Cast(new GeneralScalarExpression("+", Array[Expression](id, null)), IntegerType) ->
        "CAST(+(id, null) AS INT)"
    ).foreach { case (e, rendered) =>
      assert(describeSortOrder(key(transform("f", e))) === s"f($rendered) ASC NULLS FIRST")
      assert(emitted(transform("f", e)).isEmpty, rendered)
    }
  }

  test("a connector expression that fails to render is described, and has no clause form") {
    val broken = new Transform {
      override def name(): String = "broken"
      override def references(): Array[NamedReference] = Array.empty
      override def arguments(): Array[Expression] = throw new IllegalStateException("broken")
      override def toString: String = "broken()"
    }
    assert(describeSortOrder(key(broken)) === "broken() ASC NULLS FIRST")
    assert(emitted(broken).isEmpty)
  }

  test("DESCRIBE renders the children of a connector expression with their type and quoting") {
    val connector = new ConnectorReference("order-id")
    val date = literal(LocalDate.of(1970, 1, 1))
    val partitionPredicate = new PartitionPredicate {
      override def references(): Array[NamedReference] = Array(id)
      override def eval(partitionKey: InternalRow): Boolean = true
    }
    Seq(
      new GeneralScalarExpression("+", Array[Expression](connector, literal(1))) ->
        "`order-id` + 1",
      new GetArrayItem(connector, literal(0), true) -> "`order-id`[0]",
      new VariantGet(connector, "$['a']", IntegerType, true, null) ->
        "variant_get(`order-id`, '$[\\'a\\']', int)",
      sort(date, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST) ->
        "DATE '1970-01-01' ASC NULLS FIRST",
      partitionPredicate -> "PARTITION_PREDICATE()"
    ).foreach { case (e, rendered) =>
      assert(describeSortOrder(key(transform("f", e))) === s"f($rendered) ASC NULLS FIRST")
    }
  }

  test("DESCRIBE renders an identity transform inside a key as a transform") {
    assert(describeSortOrder(key(transform("f", IdentityTransform(id)))) ===
      "f(identity(id)) ASC NULLS FIRST")
    assert(describeSortOrder(key(IdentityTransform(id))) === "id ASC NULLS FIRST")
  }

  test("DESCRIBE renders a literal inside a connector expression with its type") {
    val date = literal(LocalDate.of(1970, 1, 1))
    val e = new GeneralScalarExpression("+", Array[Expression](id, date))
    assert(describeSortOrder(key(transform("f", e))) ===
      "f(id + DATE '1970-01-01') ASC NULLS FIRST")
  }

  test("a reference renders its quoted field names") {
    assert(describeSortOrder(key(new ConnectorReference("a", "order-id"))) ===
      "a.`order-id` ASC NULLS FIRST")
    assert(describeSortOrder(key(transform("f", new ConnectorReference("order-id")))) ===
      "f(`order-id`) ASC NULLS FIRST")
  }
}
