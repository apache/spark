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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.variant._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

trait PushVariantIntoScanSuiteBase extends SharedSparkSession {
  override def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.PUSH_VARIANT_INTO_SCAN.key, "true")

  // Whether the reader-deferral tests should exercise the V2 read path. Subclasses override.
  protected def useV2: Boolean

  // Write a parquet dataset via V1, then expose it as the temp view `T`. The view's read path is
  // V2 when `useV2`, V1 otherwise. Use this for tests that need to actually execute a scan and
  // compare V1 vs V2 behavior.
  protected def withVariantParquetData(schema: String, inserts: String*)(body: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      // External (LOCATION) table, so `withTable` only drops the catalog entry - the parquet
      // files at `path` survive for the subsequent V2 read.
      withTable("temp_variant_setup") {
        sql(s"create table temp_variant_setup ($schema) using PARQUET location '$path'")
        inserts.foreach(values => sql(s"insert into temp_variant_setup values $values"))
      }
      val sourceListConf: Seq[(String, String)] =
        if (useV2) Seq(SQLConf.USE_V1_SOURCE_LIST.key -> "") else Nil
      withSQLConf(sourceListConf: _*) {
        spark.read.parquet(path).createOrReplaceTempView("T")
        try body finally spark.catalog.dropTempView("T")
      }
    }
  }

  // A single parquet-backed table for `withVariantParquetTables`: the temp view name to expose, its
  // column schema, and the row literals to insert.
  protected case class VariantTable(view: String, schema: String, inserts: Seq[String])

  // Like `withVariantParquetData` but sets up several parquet tables at once, each exposed as a
  // temp view under the current read path (V1 or V2, per `useV2`). Used by the join tests, which
  // need a variant fact table plus dimension tables. Each table is written via V1 parquet to an
  // external location, then re-read as a view so the V2 suites exercise the DSv2 scan.
  protected def withVariantParquetTables(tables: VariantTable*)(body: => Unit): Unit = {
    withTempPath { dir =>
      val setupNames = tables.map(t => s"temp_setup_${t.view}")
      withTable(setupNames: _*) {
        tables.zip(setupNames).foreach { case (t, setupName) =>
          val path = s"${dir.getCanonicalPath}/${t.view}"
          sql(s"create table $setupName (${t.schema}) using PARQUET location '$path'")
          t.inserts.foreach(values => sql(s"insert into $setupName values $values"))
        }
      }
      val sourceListConf: Seq[(String, String)] =
        if (useV2) Seq(SQLConf.USE_V1_SOURCE_LIST.key -> "") else Nil
      withSQLConf(sourceListConf: _*) {
        tables.foreach { t =>
          spark.read.parquet(s"${dir.getCanonicalPath}/${t.view}").createOrReplaceTempView(t.view)
        }
        try body finally tables.foreach(t => spark.catalog.dropTempView(t.view))
      }
    }
  }

  // Find the scan relation (V1 or V2) whose output contains a column named `columnName`, and return
  // that column's data type. Used by multi-scan join tests to inspect the fact table's variant
  // column. Fails if zero or more than one matching scan is found.
  protected def scanColumnType(plan: LogicalPlan, columnName: String): DataType = {
    val matching = plan.collect {
      case s: DataSourceV2ScanRelation if s.output.exists(_.name == columnName) => s.output
      case l: LogicalRelation if l.output.exists(_.name == columnName) => l.output
    }
    assert(matching.length == 1,
      s"Expected exactly one scan with a column named '$columnName' but found " +
        s"${matching.length}:\n$plan")
    matching.head.find(_.name == columnName).get.dataType
  }

  // Assert that a variant column shredded to a struct and (optionally) that it has no full-variant
  // slot -- i.e. the whole blob is not read.
  protected def assertShreddedStruct(
      dataType: DataType, expectFullVariantSlot: Boolean = false): StructType = {
    val struct = dataType match {
      case s: StructType => s
      case other => fail(s"Expected the variant column shredded to a struct, but got $other")
    }
    val hasFullVariantSlot = struct.fields.exists(_.dataType.isInstanceOf[VariantType])
    assert(hasFullVariantSlot == expectFullVariantSlot,
      s"Expected full-variant slot present=$expectFullVariantSlot, but got $struct")
    struct
  }

  protected def localTimeZone = spark.sessionState.conf.sessionLocalTimeZone

  // Return a `StructField` with the expected `VariantMetadata`.
  protected def field(ordinal: Int, dataType: DataType, path: String,
                    failOnError: Boolean = true, timeZone: String = localTimeZone): StructField =
    StructField(ordinal.toString, dataType,
      metadata = VariantMetadata(path, failOnError, timeZone).toMetadata)

  // Validate an `Alias` expression has the expected name and child.
  protected def checkAlias(expr: Expression, expectedName: String, expected: Expression): Unit = {
    expr match {
      case Alias(child, name) =>
        assert(name == expectedName)
        assert(child == expected)
      case _ => fail()
    }
  }

  // Locate the single scan relation in an optimized plan and return its output, regardless of the
  // read path (V1 `LogicalRelation` or V2 `DataSourceV2ScanRelation`). Lets a shared test assert
  // shredding for both the V1 and V2 pushdown rules.
  protected def scanRelationOutput(plan: LogicalPlan): Seq[Attribute] = {
    val outputs = plan.collect {
      case s: DataSourceV2ScanRelation => s.output
      case l: LogicalRelation => l.output
    }
    assert(outputs.length == 1,
      s"Expected exactly one scan relation but found ${outputs.length}:\n$plan")
    outputs.head
  }

  test("aggregate function argument variant_get is hoisted below the aggregate and shredded") {
    // `max(variant_get(v, '$.price'))` lives inside the aggregate function, above the aggregate
    // barrier that PhysicalOperation-based pushdown cannot see through. PullOutVariantExtractions
    // hoists it into a Project directly below the Aggregate so both the V1 and V2 pushdown rules
    // shred `v` to just the `$.price` slot, with no full-variant slot (the raw column is never
    // read). Runs under both the V1 and V2 pushdown paths.
    withVariantParquetData(
      "v variant, v2 variant, name string",
      "(parse_json('{\"price\": 10}'), parse_json('{\"junk\": 1}'), 'a')",
      "(parse_json('{\"price\": 30}'), parse_json('{\"junk\": 2}'), 'a')",
      "(parse_json('{\"price\": 20}'), parse_json('{\"junk\": 3}'), 'b')") {
      val query = "select name, max(variant_get(v, '$.price', 'int')) as mx from T group by name"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected) // a -> 30, b -> 20
      val output = scanRelationOutput(sql(query).queryExecution.optimizedPlan)
      val v = output.find(_.name == "v").getOrElse(fail(s"v missing from ${output.map(_.name)}"))
      val vStruct = v.dataType match {
        case s: StructType => s
        case other => fail(s"Expected v shredded to struct, but got $other")
      }
      assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
        s"Expected no full-variant slot, but got $vStruct")
    }
  }

  test("sort order variant_get is hoisted below the sort and shredded") {
    // The sort key variant_get(v, '$.price') lives in Sort.order. PullOutVariantExtractions hoists
    // it into a Project below the Sort; since only `name` is selected (v is not otherwise live
    // above the Sort), v shreds to the $.price slot with no full-variant slot. Runs under V1/V2.
    withVariantParquetData(
      "v variant, name string",
      "(parse_json('{\"price\": 3}'), 'x')",
      "(parse_json('{\"price\": 1}'), 'z')",
      "(parse_json('{\"price\": 2}'), 'y')") {
      val query = "select name from T order by variant_get(v, '$.price', 'int')"
      val expectedOrder = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect().map(_.getString(0)).toList
      }
      assert(expectedOrder == List("z", "y", "x"), s"baseline sanity: $expectedOrder")
      assert(sql(query).collect().map(_.getString(0)).toList == expectedOrder,
        "ORDER BY variant_get produced the wrong row order")
      val output = scanRelationOutput(sql(query).queryExecution.optimizedPlan)
      val vStruct = output.find(_.name == "v").get.dataType match {
        case s: StructType => s
        case other => fail(s"Expected v shredded to struct, but got $other")
      }
      assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
        s"Expected no full-variant slot, but got $vStruct")
    }
  }

  test("bare/root sort (SELECT *): order-key variant_get is hoisted and shredded") {
    // SELECT * keeps v in the query output, so the sort key variant_get(v, '$.price') lives in a
    // bare/root Sort with no narrowing Project above it (the analyzer adds no Project since every
    // order column is already selected). PullOutVariantExtractions handles this via rewriteBareSort
    // in its second pass. Because v is in the SELECT * output it stays live, so v shreds to a
    // multi-slot struct WITH a full-variant slot (the raw blob is still materialized) plus the
    // typed $.price slot the sort reads. Runs under V1/V2.
    withVariantParquetData(
      "v variant, name string",
      "(parse_json('{\"price\": 3}'), 'x')",
      "(parse_json('{\"price\": 1}'), 'z')",
      "(parse_json('{\"price\": 2}'), 'y')") {
      val query = "select * from T order by variant_get(v, '$.price', 'int')"
      // Order-sensitive: compare the non-variant `name` column sequence.
      val expectedOrder = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect().map(_.getString(1)).toList
      }
      assert(expectedOrder == List("z", "y", "x"), s"baseline sanity: $expectedOrder")
      assert(sql(query).collect().map(_.getString(1)).toList == expectedOrder,
        "SELECT * ORDER BY variant_get produced the wrong row order")
      val output = scanRelationOutput(sql(query).queryExecution.optimizedPlan)
      assertShreddedStruct(
        output.find(_.name == "v").get.dataType, expectFullVariantSlot = true)
    }
  }

  test("bare/root sort (SELECT id, v): order-key variant_get is hoisted and shredded") {
    // Explicitly selecting the variant column (SELECT id, v) produces the same bare/root Sort shape
    // as SELECT *: no narrowing Project above the Sort. Exercises rewriteBareSort's second pass;
    // v stays live (it is selected) so it shreds with a full-variant slot plus the $.b slot.
    withVariantParquetData(
      "id int, v variant",
      "(1, parse_json('{\"b\": 3}'))",
      "(2, parse_json('{\"b\": 1}'))",
      "(3, parse_json('{\"b\": 2}'))") {
      val query = "select id, v from T order by variant_get(v, '$.b', 'int')"
      val expectedOrder = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect().map(_.getInt(0)).toList
      }
      assert(expectedOrder == List(2, 3, 1), s"baseline sanity: $expectedOrder")
      assert(sql(query).collect().map(_.getInt(0)).toList == expectedOrder,
        "SELECT id, v ORDER BY variant_get produced the wrong row order")
      val output = scanRelationOutput(sql(query).queryExecution.optimizedPlan)
      assertShreddedStruct(
        output.find(_.name == "v").get.dataType, expectFullVariantSlot = true)
    }
  }

  test("bare sort over a join (SELECT *): order-key extraction pushed through the join") {
    // A bare/root Sort whose child is a Join, with all join columns selected (no narrowing Project
    // above the Sort). The hoisted order-key alias must be pushed DOWN through the join to the ss
    // scan side. `data` is in the query output, so it shreds with a full-variant slot plus the
    // $.qty slot. Runs under V1/V2.
    withVariantParquetTables(
      factTable(),
      VariantTable("DIM", "k int, name string", Seq("(1, 'x')", "(2, 'y')"))) {
      val query =
        "select ss.k, ss.data, d.name from SS ss join DIM d on ss.k = d.k " +
          "order by variant_get(ss.data, '$.qty', 'double')"
      val expectedOrder = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect().map(_.getInt(0)).toList
      }
      assert(sql(query).collect().map(_.getInt(0)).toList == expectedOrder,
        s"SELECT with join columns ORDER BY variant_get produced the wrong row order: " +
          s"got ${sql(query).collect().map(_.getInt(0)).toList}, expected $expectedOrder")
      assertShreddedStruct(
        scanColumnType(sql(query).queryExecution.optimizedPlan, "data"),
        expectFullVariantSlot = true)
    }
  }

  test("cast(v as variant) in aggregate function arg is not hoisted") {
    // The aggregate argument is `cast(v as variant)`: a whole-variant read whose sole requested
    // field targets VariantType. PullOutVariantExtractions must NOT hoist it (isHoistable excludes
    // VariantType-target casts) -- hoisting would add a Project + `_ve` alias for no shredding
    // benefit (the pushdown drops a lone VariantType slot anyway, see SPARK-57499). Assert the rule
    // introduces no hoist alias, so the plan matches what the pull-out rule being off would give.
    // Plan-shape only (no execution): a lone-VariantType shred is a separate, pre-existing concern.
    // Runs under both the V1 and V2 pushdown paths.
    withVariantParquetData(
      "v variant, name string",
      "(parse_json('{\"price\": 10}'), 'a')",
      "(parse_json('{\"price\": 20}'), 'b')") {
      val query = "select name, count(cast(v as variant)) as c from T group by name"
      val plan = sql(query).queryExecution.optimizedPlan
      val hoistAlias = plan.exists(_.expressions.exists(_.exists {
        case a: Alias => a.name.startsWith("_ve")
        case _ => false
      }))
      assert(!hoistAlias,
        s"Expected no `_ve` hoist alias for a VariantType-target cast, but plan was:\n$plan")
    }
  }

  test("two-arg variant_get (returns VariantType) in aggregate function arg is not hoisted") {
    // variant_get(v, '$.price') with no target type returns VariantType. isHoistable must NOT hoist
    // it: the pushdown drops a lone VariantType requested field (SPARK-57499), so hoisting
    // produces a `_ve` alias for no shredding benefit. It would also re-match on the second
    // idempotence pass, breaking the Once-batch check. Assert no `_ve` alias is introduced.
    withVariantParquetData(
      "v variant, name string",
      "(parse_json('{\"price\": 10}'), 'a')",
      "(parse_json('{\"price\": 20}'), 'b')") {
      val query = "select name, count(variant_get(v, '$.price')) as c from T group by name"
      val plan = sql(query).queryExecution.optimizedPlan
      val hoistAlias = plan.exists(_.expressions.exists(_.exists {
        case a: Alias => a.name.startsWith("_ve")
        case _ => false
      }))
      assert(!hoistAlias,
        s"Expected no `_ve` hoist alias for a two-arg variant_get (VariantType result), " +
          s"but plan was:\n$plan")
    }
  }

  test("listagg(DISTINCT v:a::string) WITHIN GROUP (ORDER BY v:a::string) does not break " +
      "Once-batch idempotence") {
    // Regression test for the idempotence bug: `v:a::string` desugars to
    // `cast(variant_get(v, '$.a') as string)`, where the inner two-arg variant_get returns
    // VariantType. Excluding that two-arg form from hoisting (isHoistable's VariantGet branch)
    // leaves the whole `cast(variant_get(...))` untouched -- so nothing is hoisted here and there
    // is no VariantType `_ve` alias for the Cast branch to re-hoist on the next pass. Without the
    // exclusion the VariantGet would hoist to `_ve0: VariantType`, then `cast(_ve0 as string)`
    // would re-match, producing a different plan and crashing with
    // "Once strategy's idempotence is broken".
    withVariantParquetData(
      "v variant",
      "(parse_json('{\"a\": \"x\"}'))",
      "(parse_json('{\"a\": \"y\"}'))",
      "(parse_json('{\"a\": \"x\"}'))") {
      val query =
        "select listagg(distinct variant_get(v, '$.a', 'string'), ',') " +
          "within group (order by variant_get(v, '$.a', 'string')) from T"
      // Must execute without an idempotence exception and return the expected deduplicated result.
      checkAnswer(sql(query), Seq(org.apache.spark.sql.Row("x,y")))
    }
  }

  test("sort key hoisted while a different projected extraction stays above the sort: correct") {
    // The projected extraction variant_get(v, '$.a') sits in the select list (above the Sort,
    // lifted by PhysicalOperation), while the sort key variant_get(v, '$.b') is hoisted below the
    // Sort. Because the projectList still references v raw, the hoist must NOT drop v -- it stays
    // live above the Sort so the projected extraction can be computed. This guards the liveness
    // computation in rewriteSortUnderProject: results and row order must both be correct. Runs
    // under both the V1 and V2 pushdown paths.
    withVariantParquetData(
      "v variant, name string",
      "(parse_json('{\"a\": 100, \"b\": 3}'), 'x')",
      "(parse_json('{\"a\": 200, \"b\": 1}'), 'z')",
      "(parse_json('{\"a\": 300, \"b\": 2}'), 'y')") {
      val query =
        "select name, variant_get(v, '$.a', 'int') as a " +
          "from T order by variant_get(v, '$.b', 'int')"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect().toSeq
      }
      // Baseline sanity: ordered by $.b ascending -> z(1), y(2), x(3) with their $.a values.
      assert(expected.map(r => (r.getString(0), r.getInt(1))) ==
        Seq(("z", 200), ("y", 300), ("x", 100)), s"baseline sanity: $expected")
      // Order-sensitive comparison: a mis-hoist that dropped v or mis-shredded would change the
      // projected $.a value or the row order, which an order-insensitive checkAnswer would miss.
      assert(sql(query).collect().toSeq.map(r => (r.getString(0), r.getInt(1))) ==
        expected.map(r => (r.getString(0), r.getInt(1))),
        "projected extraction / order incorrect after hoisting the sort key")
    }
  }

  test("group by + aggregate hoist fuses into one Project, no stacked Project") {
    // GROUP BY variant_get(v, '$.k') is relocated below the Aggregate by PullOutGroupingExpressions
    // (creating a Project), and the aggregated variant_get(v, '$.price') is hoisted by
    // PullOutVariantExtractions. The hoist must FUSE its aliases into that existing Project rather
    // than stack a second one -- the earlyScanPushDownRules batch runs Once with no CollapseProject
    // to flatten it afterward. Assert no two directly-stacked Projects survive in the optimized
    // plan. Runs under both the V1 and V2 pushdown paths.
    withVariantParquetData(
      "v variant",
      "(parse_json('{\"k\": \"a\", \"price\": 10}'))",
      "(parse_json('{\"k\": \"a\", \"price\": 30}'))",
      "(parse_json('{\"k\": \"b\", \"price\": 20}'))") {
      val query = "select variant_get(v, '$.k', 'string') as k, " +
        "max(variant_get(v, '$.price', 'int')) as mx " +
        "from T group by variant_get(v, '$.k', 'string')"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected) // a -> 30, b -> 20
      val plan = sql(query).queryExecution.optimizedPlan
      val stacked = plan.exists {
        case Project(_, _: Project) => true
        case _ => false
      }
      assert(!stacked,
        s"Expected no directly-stacked Project nodes after hoist fusion, but got:\n$plan")
    }
  }

  // Returns true iff `t` or any of its causes is an INVALID_VARIANT_CAST error. The failure may
  // surface directly or be wrapped in a task failure.
  protected def hasCastCondition(t: Throwable): Boolean = t match {
    case null => false
    case s: org.apache.spark.SparkThrowable if s.getCondition == "INVALID_VARIANT_CAST" => true
    case _ => hasCastCondition(t.getCause)
  }

  test(s"Strict cast wraps with cast-error-deferred error") {
    withTable("T") {
      withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "true") {
        sql("create table T (v variant) using parquet")
        sql("select cast(v as int) as a, try_variant_get(v, '$.b', 'string') as b from T")
          .queryExecution.optimizedPlan match {
          case Project(projectList, l: LogicalRelation) =>
            val output = l.output
            val v = output(0)
            // Strict cast should be wrapped with `UnwrapVariantCastError` over the sibling
            // companion field whose `castErrorFor` metadata names the data field.
            projectList(0) match {
              case Alias(UnwrapVariantCastError(
                  GetStructField(_, errOrd, _), GetStructField(_, 0, _)), "a") =>
                assert(errOrd == 2, s"Expected companion ordinal 2, got $errOrd")
              case other => fail(s"Unexpected projection 0: $other")
            }
            // try_variant_get is non-strict and should NOT be wrapped.
            projectList(1) match {
              case Alias(GetStructField(_, 1, _), "b") =>
              case other => fail(s"Unexpected projection 1: $other")
            }
            val expected = StructType(Array(
              field(0, IntegerType, "$", failOnError = true),
              field(1, StringType, "$.b", failOnError = false),
              StructField("2", StringType,
                metadata = VariantMetadata.castErrorCompanionMetadata("0"))
            ))
            assert(v.dataType == expected, s"Got ${v.dataType}")
          case other => fail(s"Unexpected plan: $other")
        }
      }
    }
  }

  test(s"Cast-error companion is skipped for full-variant access") {
    withTable("T") {
      withSQLConf(
        SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "true") {
        sql("create table T (v variant) using parquet")
        // Selecting `v` alone produces only the full-variant request. cast-to-variant never
        // fails, so no cast-error companion should be emitted.
        sql("select v from T").queryExecution.optimizedPlan match {
          case Project(_, l: LogicalRelation) =>
            val v = l.output(0)
            val expected = StructType(Array(
              field(0, VariantType, "$", timeZone = "UTC")
            ))
            assert(v.dataType == expected, s"Got ${v.dataType}")
          case other => fail(s"Unexpected plan: $other")
        }
      }
    }
  }

  test(s"Reader defers strict-cast errors when cast-error companion is present") {
    // Row 0: number 1 (LONG in variant) -> cast(v as int) succeeds.
    // Row 1: string -> cast(v as int) would raise INVALID_VARIANT_CAST. With the deferral, the
    //                  surrounding `if(schema_of_variant(v) = 'BIGINT', cast(v as int), null)`
    //                  short-circuits to null before the error is observed.
    withVariantParquetData("v variant",
        "(parse_json('1'))",
        "(parse_json('\"hello\"'))") {
      val query =
        "select if(schema_of_variant(v) = 'BIGINT', cast(v as int), null) as a from T"

      // Without the deferral, the strict cast pushed into the scan raises at the failing row
      // even though the `if` would have filtered it out.
      withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "false") {
        val ex = intercept[Exception](sql(query).collect())
        assert(hasCastCondition(ex), s"Expected INVALID_VARIANT_CAST, got $ex")
      }

      // With the deferral, the strict cast emits a cast-error companion and the `if`
      // short-circuits before the failing row is consumed.
      withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "true") {
        val rows = sql(query).collect()
        val values = rows.map(r => if (r.isNullAt(0)) null else r.getInt(0).asInstanceOf[Any])
          .toSet
        assert(values == Set(1, null), s"Got ${values.mkString(",")}")
      }
    }
  }

  test(s"Reader defers strict-cast errors for struct target") {
    // Row 0: object with int field -> cast(v as struct<x int>) succeeds.
    // Row 1: scalar -> cast(v as struct<x int>) would raise (wrong kind).
    withVariantParquetData("v variant",
        "(parse_json('{\"x\": 1}'))",
        "(parse_json('\"hello\"'))") {
      val query =
        "select if(schema_of_variant(v) like 'OBJECT<%>', cast(v as struct<x: int>), null) as a " +
          "from T"
      withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "true") {
        val rows = sql(query).collect()
        val xs = rows.map { r =>
          if (r.isNullAt(0)) null else r.getStruct(0).getInt(0).asInstanceOf[Any]
        }.toSet
        assert(xs == Set(1, null), s"Got ${xs.mkString(",")}")
      }
    }
  }

  test(s"Reader defers strict-cast errors for array target") {
    // Row 0: array of ints -> cast(v as array<int>) succeeds.
    // Row 1: scalar -> cast(v as array<int>) wrong-kind failure.
    withVariantParquetData("v variant",
        "(parse_json('[1, 2, 3]'))",
        "(parse_json('\"hello\"'))") {
      val query =
        "select if(schema_of_variant(v) like 'ARRAY<%>', cast(v as array<int>), null) as a " +
          "from T"
      withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "true") {
        val rows = sql(query).collect()
        val arrs = rows.map { r =>
          if (r.isNullAt(0)) null else r.getList[Int](0).toArray.toSeq
        }.toSet
        assert(arrs == Set(Seq(1, 2, 3), null), s"Got ${arrs.mkString(",")}")
      }
    }
  }

  test(s"Reader surfaces deferred error for array target with inner-element failure") {
    // Row 0: heterogeneous array; cast(v as array<int>) fails on the inner string element.
    // With deferred errors enabled, the failure must surface when the row is consumed by the
    // outer expression -- i.e., the element-level companion buffer was correctly aggregated to
    // the outer row.
    withVariantParquetData("v variant",
        "(parse_json('[1, \"abc\"]'))") {
      withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "true") {
        val ex = intercept[Exception](sql("select cast(v as array<int>) from T").collect())
        assert(hasCastCondition(ex), s"Expected INVALID_VARIANT_CAST, got $ex")
      }
    }
  }

  test(s"Reader surfaces deferred error for struct target with field cast failure") {
    // Force the writer to shred `x` as int. The inner string `"abc"` lands in the unshredded
    // `value` part, and `cast(v as struct<x: int>)` reads the int via the shredded path, which
    // exercises `SparkShreddingUtils.getFieldsToExtract` / `assembleVariantStruct` with the new
    // companion-field pairing.
    withSQLConf(
      SQLConf.VARIANT_WRITE_SHREDDING_ENABLED.key -> "true",
      SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> "x int") {
      withVariantParquetData("v variant",
          "(parse_json('{\"x\": \"abc\"}'))") {
        withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "true") {
          val ex =
            intercept[Exception](sql("select cast(v as struct<x: int>) from T").collect())
          assert(hasCastCondition(ex), s"Expected INVALID_VARIANT_CAST, got $ex")
        }
      }
    }
  }

  test(s"Reader defers strict-cast errors through AND/OR short-circuit") {
    // Row 0: number 1 (LONG in variant) -> cast(v as int) succeeds.
    // Row 1: string -> cast(v as int) would raise INVALID_VARIANT_CAST.
    //
    // The strict cast is a child of an `AND`/`OR` that is projected as a boolean value. The
    // `AND`/`OR` must be evaluated lazily/left-to-right with short-circuit: when the left operand
    // already decides the result (false for `AND`, true for `OR`) the right operand (the wrapped
    // cast) is not consumed, so the deferred cast error on the string row must not surface.
    withVariantParquetData("v variant",
        "(parse_json('1'))",
        "(parse_json('\"hello\"'))") {
      // For each case: the projected expression, and the expected (sorted) values with deferral on.
      // - AND: row 0 = 'BIGINT'='BIGINT' (true) AND 1 > 5 (false) -> false;
      //        row 1 = 'STRING'='BIGINT' (false) -> false (cast deferred, never consumed).
      // - OR:  row 0 = 'BIGINT'='STRING' (false) OR 1 > 5 (false) -> false;
      //        row 1 = 'STRING'='STRING' (true) -> true (cast deferred, never consumed).
      val cases = Seq(
        "schema_of_variant(v) = 'BIGINT' and cast(v as int) > 5" -> Seq(false, false),
        "schema_of_variant(v) = 'STRING' or cast(v as int) > 5" -> Seq(false, true))

      for ((expr, expected) <- cases) {
        val query = s"select $expr as a from T"

        // Without the deferral, the strict cast pushed into the scan raises at the failing row even
        // though the `AND`/`OR` would have short-circuited past it.
        withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "false") {
          val ex = intercept[Exception](sql(query).collect())
          assert(hasCastCondition(ex), s"[$expr] Expected INVALID_VARIANT_CAST, got $ex")
        }

        // With the deferral, the short-circuit happens before the failing row is consumed.
        // Read order is not guaranteed, so compare the sorted values.
        withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_DEFER_CAST_ERROR.key -> "true") {
          val values = sql(query).collect().map(_.getBoolean(0)).sorted.toSeq
          assert(values == expected, s"[$expr] Got ${values.mkString(",")}")
        }
      }
    }
  }

  // A variant fact table `SS(k int, data variant)` used by the push-through-join tests below.
  private def factTable(view: String = "SS"): VariantTable = VariantTable(
    view, "k int, data variant",
    Seq(
      "(1, parse_json('{\"qty\": 10, \"id\": \"a\"}'))",
      "(1, parse_json('{\"qty\": 30, \"id\": \"a\"}'))",
      "(2, parse_json('{\"qty\": 20, \"id\": \"b\"}'))"))

  test("aggregate over a single inner join: variant hoisted through the join and shredded") {
    // AVG(variant_get(ss.data, '$.qty')) sits in an Aggregate whose child is a Join. The extraction
    // is hoisted, then pushed down through the join onto the ss side so it lands directly above the
    // ss scan. `data` is used ONLY via extractions (aggregate arg + GROUP BY key), so it shreds
    // with no full-variant slot -- the whole blob is never read. Runs under both V1 and V2.
    withVariantParquetTables(
      factTable(),
      VariantTable("DIM", "k int, name string", Seq("(1, 'x')", "(2, 'y')"))) {
      val query =
        "select variant_get(ss.data, '$.id', 'string') as id, " +
          "avg(variant_get(ss.data, '$.qty', 'double')) as avg_qty " +
          "from SS ss join DIM d on ss.k = d.k " +
          "group by variant_get(ss.data, '$.id', 'string')"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected) // a -> 20.0, b -> 20.0
      assertShreddedStruct(scanColumnType(sql(query).queryExecution.optimizedPlan, "data"))
    }
  }

  test("aggregate over chained joins: variant pushed through all join levels and shredded") {
    // The ss variant is joined to three dimensions. The aggregate-argument extractions must be
    // pushed down through EVERY join level to reach the ss scan. This guards the recursion in
    // pushSideAliases: a single non-recursive hoist would leave the extraction above the outermost
    // join and `data` would still be read raw. Runs under both V1 and V2.
    withVariantParquetTables(
      factTable(),
      VariantTable("D1", "k int, a string", Seq("(1, 'p')", "(2, 'q')")),
      VariantTable("D2", "k int, b string", Seq("(1, 'r')", "(2, 's')")),
      VariantTable("D3", "k int, c string", Seq("(1, 't')", "(2, 'u')"))) {
      val query =
        "select variant_get(ss.data, '$.id', 'string') as id, " +
          "max(variant_get(ss.data, '$.qty', 'double')) as max_qty " +
          "from SS ss " +
          "join D1 on ss.k = D1.k join D2 on ss.k = D2.k join D3 on ss.k = D3.k " +
          "group by variant_get(ss.data, '$.id', 'string')"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected) // a -> 30.0, b -> 20.0
      assertShreddedStruct(scanColumnType(sql(query).queryExecution.optimizedPlan, "data"))
    }
  }

  test("variant used raw and extracted across a join: keeps a full-variant slot") {
    // ss.data is both selected raw (needs the whole blob) and extracted. After the extraction is
    // pushed to the ss scan side, `data` stays live (still referenced raw), so it shreds to the
    // typed slot PLUS a full-variant slot -- the raw value is preserved. Runs under both V1 and V2.
    withVariantParquetTables(
      factTable(),
      VariantTable("DIM", "k int, name string", Seq("(1, 'x')", "(2, 'y')"))) {
      val query =
        "select ss.data as raw, variant_get(ss.data, '$.qty', 'double') as qty " +
          "from SS ss join DIM d on ss.k = d.k"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected)
      val struct =
        assertShreddedStruct(
          scanColumnType(sql(query).queryExecution.optimizedPlan, "data"),
          expectFullVariantSlot = true)
      // One typed slot ($.qty) plus the full-variant slot.
      assert(struct.fields.length == 2, s"Expected two slots, but got $struct")
    }
  }

  test("aggregate over a left outer join: extraction on the preserved side is shredded") {
    // ss is the preserved (left) side of a LEFT JOIN. Its variant extraction pushes onto the left
    // side; unmatched rows (null-padded on the right) do not affect the ss-side extraction. Assert
    // no full-variant slot and correct results including the unmatched row. Runs under V1 and V2.
    withVariantParquetTables(
      factTable(),
      VariantTable("DIM", "k int, name string", Seq("(1, 'x')"))) {
      val query =
        "select variant_get(ss.data, '$.id', 'string') as id, " +
          "max(variant_get(ss.data, '$.qty', 'double')) as max_qty " +
          "from SS ss left join DIM d on ss.k = d.k " +
          "group by variant_get(ss.data, '$.id', 'string')"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected) // a -> 30.0 (k=1 matched), b -> 20.0 (k=2 unmatched)
      assertShreddedStruct(scanColumnType(sql(query).queryExecution.optimizedPlan, "data"))
    }
  }

  test("pull-out flag off reads the whole variant across a join") {
    // With PUSH_VARIANT_INTO_SCAN_PULL_OUT_EXTRACTIONS disabled, the aggregate-argument extractions
    // are not hoisted or pushed through the join. `ss.data` flows up through the join as a bare
    // attribute, so the scan reads the whole blob -- either left raw as VariantType, or shredded to
    // a single full-variant slot (struct<0:variant>). Either way there is no shredding benefit.
    // Guards the config gate for the new join push-through path. Runs under both V1 and V2.
    withVariantParquetTables(
      factTable(),
      VariantTable("DIM", "k int, name string", Seq("(1, 'x')", "(2, 'y')"))) {
      val query =
        "select variant_get(ss.data, '$.id', 'string') as id, " +
          "avg(variant_get(ss.data, '$.qty', 'double')) as avg_qty " +
          "from SS ss join DIM d on ss.k = d.k " +
          "group by variant_get(ss.data, '$.id', 'string')"
      withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN_PULL_OUT_EXTRACTIONS.key -> "false") {
        val dataType = scanColumnType(sql(query).queryExecution.optimizedPlan, "data")
        val readsWholeBlob = dataType match {
          case _: VariantType => true
          case s: StructType => s.fields.exists(_.dataType.isInstanceOf[VariantType])
          case _ => false
        }
        assert(readsWholeBlob,
          s"Expected data read as a whole blob with the pull-out flag off, but got $dataType")
      }
    }
  }

  test("aggregate over a left semi join: left-side extraction pushed and shredded") {
    // A LEFT SEMI join's output is only the left (ss) columns. The ss-side aggregate extraction is
    // pushed onto the left side and shreds with no full-variant slot; the semi join's existence
    // semantics are unchanged. Runs under both V1 and V2.
    withVariantParquetTables(
      factTable(),
      VariantTable("DIM", "k int", Seq("(1)"))) {
      val query =
        "select variant_get(ss.data, '$.id', 'string') as id, " +
          "max(variant_get(ss.data, '$.qty', 'double')) as max_qty " +
          "from SS ss left semi join DIM d on ss.k = d.k " +
          "group by variant_get(ss.data, '$.id', 'string')"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected) // only k=1 rows survive -> a -> 30.0
      assertShreddedStruct(scanColumnType(sql(query).queryExecution.optimizedPlan, "data"))
    }
  }

  test("left outer join: extraction on the NULLABLE (right) side is pushed and value-preserving") {
    // The extraction reads the DIM (right) variant, which is the NULLABLE side of a LEFT OUTER
    // join: unmatched left rows are null-padded on the right. Pushing the extraction below the join
    // computes variant_get BEFORE null-padding; the join then null-pads the `_ve` alias for
    // unmatched rows. This must equal the baseline, where variant_get(NULL) is evaluated ABOVE the
    // join for those rows -- i.e. null-padding commutes with the extraction (variant_get(NULL) =
    // NULL). Asserting correctness here is the real point; also assert DIM.data shreds. The `ss`
    // (left) side has its own unrelated extraction so both scans are exercised. Runs under V1/V2.
    withVariantParquetTables(
      factTable(),
      VariantTable(
        "DIM", "k int, data variant",
        // Only k=1 matches SS's k in {1, 2}; SS rows with k=2 are unmatched -> right side null.
        Seq("(1, parse_json('{\"label\": \"L1\"}'))"))) {
      val query =
        "select variant_get(ss.data, '$.id', 'string') as id, " +
          "variant_get(d.data, '$.label', 'string') as label " +
          "from SS ss left join DIM d on ss.k = d.k " +
          "order by id, label"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      // Order-sensitive comparison so a wrong null for unmatched rows cannot be masked.
      assert(sql(query).collect().toSeq.map(r => (r.get(0), r.get(1))) ==
        expected.toSeq.map(r => (r.get(0), r.get(1))),
        "Nullable-side extraction over a left outer join produced wrong values")
      // Both SS.data and DIM.data are extracted only via variant_get; each shreds with no
      // full-variant slot on its own scan (both scans expose a `data` column, so assert per scan).
      val plan = sql(query).queryExecution.optimizedPlan
      val dataTypes = plan.collect {
        case s: DataSourceV2ScanRelation => s.output
        case l: LogicalRelation => l.output
      }.flatMap(_.filter(_.name == "data")).map(_.dataType)
      assert(dataTypes.length == 2, s"Expected two `data` scan columns, but got:\n$plan")
      dataTypes.foreach(dt => assertShreddedStruct(dt))
    }
  }

  test("full outer join: extractions on both (nullable) sides are pushed and value-preserving") {
    // In a FULL OUTER join both sides are nullable: unmatched rows are null-padded on the opposite
    // side. Extractions on each side push onto that side, computed before null-padding; the join
    // null-pads the resulting `_ve` aliases for the unmatched rows. Result must match the baseline
    // that evaluates variant_get above the join (variant_get(NULL) = NULL). Runs under V1 and V2.
    withVariantParquetTables(
      VariantTable(
        "L", "k int, data variant",
        Seq("(1, parse_json('{\"lv\": \"a\"}'))", "(2, parse_json('{\"lv\": \"b\"}'))")),
      VariantTable(
        "R", "k int, data variant",
        Seq("(2, parse_json('{\"rv\": \"y\"}'))", "(3, parse_json('{\"rv\": \"z\"}'))"))) {
      val query =
        "select variant_get(l.data, '$.lv', 'string') as lv, " +
          "variant_get(r.data, '$.rv', 'string') as rv " +
          "from L l full outer join R r on l.k = r.k " +
          "order by lv, rv"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      assert(sql(query).collect().toSeq.map(r => (r.get(0), r.get(1))) ==
        expected.toSeq.map(r => (r.get(0), r.get(1))),
        "Both-nullable-side extractions over a full outer join produced wrong values")
      // Both L.data and R.data are extracted only via variant_get, so each shreds with no
      // full-variant slot on its own scan. Both scans expose a column named `data`, so assert on
      // every `data` scan output rather than via the single-match scanColumnType helper.
      val plan = sql(query).queryExecution.optimizedPlan
      val dataTypes = plan.collect {
        case s: DataSourceV2ScanRelation => s.output
        case l: LogicalRelation => l.output
      }.flatMap(_.filter(_.name == "data")).map(_.dataType)
      assert(dataTypes.length == 2, s"Expected two `data` scan columns, but got:\n$plan")
      dataTypes.foreach(dt => assertShreddedStruct(dt))
    }
  }

  test("left semi join with a right-side variant_get in the condition: right side shredded") {
    // The join condition extracts from the RIGHT (DIM) variant. For a LEFT SEMI join the right side
    // is not in the join output, but the condition is still evaluated over both children, so the
    // right-side extraction is hoisted onto the right side and its `_ve` alias feeds the condition.
    // Exercises the right-side routing in rewriteJoinUnderProject for a LeftExistence join. Assert
    // correct existence semantics and that DIM.data shreds with no full-variant slot. V1 and V2.
    withVariantParquetTables(
      factTable(),
      VariantTable(
        "DIM", "dk int, data variant",
        Seq("(1, parse_json('{\"jk\": 1}'))", "(2, parse_json('{\"jk\": 9}'))"))) {
      val query =
        "select variant_get(ss.data, '$.id', 'string') as id " +
          "from SS ss left semi join DIM d " +
          "on ss.k = variant_get(d.data, '$.jk', 'int') " +
          "order by id"
      val expected = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expected) // ss.k in {1,2}; DIM jk in {1,9}; only k=1 rows survive
      // DIM.data (right side) is used only via the condition extraction, so it shreds to the $.jk
      // slot with no full-variant slot. Both SS and DIM expose a `data` column; assert both scans
      // shred (the SS-side `$.id` extraction and the DIM-side `$.jk` condition extraction).
      val plan = sql(query).queryExecution.optimizedPlan
      val dataTypes = plan.collect {
        case s: DataSourceV2ScanRelation => s.output
        case l: LogicalRelation => l.output
      }.flatMap(_.filter(_.name == "data")).map(_.dataType)
      assert(dataTypes.length == 2, s"Expected two `data` scan columns, but got:\n$plan")
      dataTypes.foreach(dt => assertShreddedStruct(dt))
    }
  }

  test("sort over a join: order-key extraction is pushed through the join and shredded") {
    // ORDER BY variant_get(ss.data, ...) sits in a Sort whose child is a Join (via the intermediate
    // Project the optimizer leaves above the join). The hoisted order-key alias must be pushed DOWN
    // through the join onto the ss side to reach the ss scan; parking it in a Project above the
    // join would leave `data` read raw (PhysicalOperation stops at the join). Only ss.k is
    // selected, so `data` is used only via the order key and shreds with no full-variant slot.
    // Runs under V1/V2.
    withVariantParquetTables(
      factTable(),
      VariantTable("DIM", "k int, name string", Seq("(1, 'x')", "(2, 'y')"))) {
      val query =
        "select ss.k from SS ss join DIM d on ss.k = d.k " +
          "order by variant_get(ss.data, '$.qty', 'double')"
      // Order-sensitive: compare the exact row sequence against the pushdown-disabled baseline.
      val expectedOrder = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect().map(_.getInt(0)).toList
      }
      assert(sql(query).collect().map(_.getInt(0)).toList == expectedOrder,
        s"ORDER BY variant_get over a join produced the wrong row order: got " +
          s"${sql(query).collect().map(_.getInt(0)).toList}, expected $expectedOrder")
      assertShreddedStruct(scanColumnType(sql(query).queryExecution.optimizedPlan, "data"))
    }
  }
}

// V1 DataSource tests with parameterized reader type
abstract class PushVariantIntoScanV1SuiteBase extends PushVariantIntoScanSuiteBase {
  protected def vectorizedReaderEnabled: Boolean
  protected def readerName: String

  override protected def useV2: Boolean = false

  override def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key,
      vectorizedReaderEnabled.toString)

  private def testOnFormats(fn: String => Unit): Unit = {
    for (format <- Seq("PARQUET")) {
      test(s"test - $format ($readerName)") {
        withTable("T") {
          fn(format)
        }
      }
    }
  }

  testOnFormats { format =>
    sql("create table T (v variant, vs struct<v1 variant, v2 variant, i int>, " +
      "va array<variant>, vd variant default parse_json('1'), s string) " +
      s"using $format")

    sql("select variant_get(v, '$.a', 'int') as a, v, cast(v as struct<b float>) as v from T")
      .queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val v = output(0)
        checkAlias(projectList(0), "a", GetStructField(v, 0))
        checkAlias(projectList(1), "v", GetStructField(v, 1))
        checkAlias(projectList(2), "v", GetStructField(v, 2))
        assert(v.dataType == StructType(Array(
          field(0, IntegerType, "$.a"),
          field(1, VariantType, "$", timeZone = "UTC"),
          field(2, StructType(Array(StructField("b", FloatType))), "$"))))
      case _ => fail()
    }

    // Validate _metadata works.
    sql("select variant_get(v, '$.a', 'int') as a, _metadata from T")
      .queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val v = output(0)
        checkAlias(projectList(0), "a", GetStructField(v, 0))
        assert(projectList(1).dataType.isInstanceOf[StructType])
      case _ => fail()
    }

    sql("select 1 from T where isnotnull(v)")
      .queryExecution.optimizedPlan match {
      case Project(projectList, Filter(condition, l: LogicalRelation)) =>
        val output = l.output
        val v = output(0)
        checkAlias(projectList(0), "1", Literal(1))
        assert(condition == IsNotNull(v))
        assert(v.dataType == StructType(Array(
          field(0, BooleanType, "$.__placeholder_field__", failOnError = false, timeZone = "UTC"))))
      case _ => fail()
    }

    sql("select variant_get(v, '$.a', 'int') + 1 as a, try_variant_get(v, '$.b', 'string') as b " +
      "from T where variant_get(v, '$.a', 'int') = 1").queryExecution.optimizedPlan match {
      case Project(projectList, Filter(condition, l: LogicalRelation)) =>
        val output = l.output
        val v = output(0)
        checkAlias(projectList(0), "a", Add(GetStructField(v, 0), Literal(1)))
        checkAlias(projectList(1), "b", GetStructField(v, 1))
        assert(condition == And(IsNotNull(v), EqualTo(GetStructField(v, 0), Literal(1))))
        assert(v.dataType == StructType(Array(
          field(0, IntegerType, "$.a"),
          field(1, StringType, "$.b", failOnError = false))))
      case _ => fail()
    }

    sql("select variant_get(vs.v1, '$.a', 'int') as a, variant_get(vs.v1, '$.b', 'int') as b, " +
      "variant_get(vs.v2, '$.a', 'int') as a, vs.i from T").queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val vs = output(1)
        val v1 = GetStructField(vs, 0, Some("v1"))
        val v2 = GetStructField(vs, 1, Some("v2"))
        checkAlias(projectList(0), "a", GetStructField(v1, 0))
        checkAlias(projectList(1), "b", GetStructField(v1, 1))
        checkAlias(projectList(2), "a", GetStructField(v2, 0))
        checkAlias(projectList(3), "i", GetStructField(vs, 2, Some("i")))
        assert(vs.dataType == StructType(Array(
          StructField("v1", StructType(Array(
            field(0, IntegerType, "$.a"), field(1, IntegerType, "$.b")))),
          StructField("v2", StructType(Array(field(0, IntegerType, "$.a")))),
          StructField("i", IntegerType))))
      case _ => fail()
    }

    def variantGet(child: Expression): Expression = VariantGet(
      child,
      path = Literal("$.a"),
      targetType = VariantType,
      failOnError = true,
      timeZoneId = Some(localTimeZone))

    // No push down if the struct containing variant is used.
    sql("select vs, variant_get(vs.v1, '$.a') as a from T").queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val vs = output(1)
        assert(projectList(0) == vs)
        checkAlias(projectList(1), "a", variantGet(GetStructField(vs, 0, Some("v1"))))
        assert(vs.dataType == StructType(Array(
          StructField("v1", VariantType),
          StructField("v2", VariantType),
          StructField("i", IntegerType))))
      case _ => fail()
    }

    // No push down for variant in array.
    sql("select variant_get(va[0], '$.a') as a from T").queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val va = output(2)
        checkAlias(projectList(0), "a", variantGet(GetArrayItem(va, Literal(0))))
        assert(va.dataType == ArrayType(VariantType))
      case _ => fail()
    }

    // No push down if variant has default value.
    sql("select variant_get(vd, '$.a') as a from T").queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val vd = output(3)
        checkAlias(projectList(0), "a", variantGet(vd))
        assert(vd.dataType == VariantType)
      case _ => fail()
    }

    // No push down if the path in variant_get is not a literal
    sql("select variant_get(v, '$.a', 'int') as a, variant_get(v, s, 'int') v2, v, " +
      "cast(v as struct<b float>) as v from T")
      .queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val v = output(0)
        val s = output(4)
        checkAlias(projectList(0), "a", GetStructField(v, 0))
        checkAlias(projectList(1), "v2", VariantGet(GetStructField(v, 1), s,
          targetType = IntegerType, failOnError = true, timeZoneId = Some(localTimeZone)))
        checkAlias(projectList(2), "v", GetStructField(v, 1))
        checkAlias(projectList(3), "v", GetStructField(v, 2))
        assert(v.dataType == StructType(Array(
          field(0, IntegerType, "$.a"),
          field(1, VariantType, "$", timeZone = "UTC"),
          field(2, StructType(Array(StructField("b", FloatType))), "$"))))
      case _ => fail()
    }
  }

  test(s"No push down for JSON ($readerName)") {
    withTable("T") {
      sql("create table T (v variant) using JSON")
      sql("select variant_get(v, '$.a') from T").queryExecution.optimizedPlan match {
        case Project(_, l: LogicalRelation) =>
          val output = l.output
          assert(output(0).dataType == VariantType)
        case _ => fail()
      }
    }
  }
}

// V1 DataSource tests - Row-based reader
class PushVariantIntoScanSuite extends PushVariantIntoScanV1SuiteBase {
  override protected def vectorizedReaderEnabled: Boolean = false
  override protected def readerName: String = "row-based reader"
}

// V1 DataSource tests - Vectorized reader
class PushVariantIntoScanVectorizedSuite extends PushVariantIntoScanV1SuiteBase {
  override protected def vectorizedReaderEnabled: Boolean = true
  override protected def readerName: String = "vectorized reader"
}

// V2 DataSource tests with parameterized reader type
abstract class PushVariantIntoScanV2SuiteBase extends QueryTest with PushVariantIntoScanSuiteBase {
  protected def vectorizedReaderEnabled: Boolean
  protected def readerName: String

  override protected def useV2: Boolean = true

  override def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key,
      vectorizedReaderEnabled.toString)

  // Locate the single DataSourceV2ScanRelation in an optimized plan, regardless of how many
  // Project/Filter nodes wrap it. This keeps scan-content assertions robust against optimizer
  // rules (e.g. CollapseProject) that may or may not collapse the identity outputProjection
  // that buildScanWithPushedVariants inserts.
  protected def findScanRelation(plan: LogicalPlan): DataSourceV2ScanRelation = {
    val scans = plan.collect { case s: DataSourceV2ScanRelation => s }
    assert(scans.length == 1,
      s"Expected exactly one DataSourceV2ScanRelation but found ${scans.length}:\n$plan")
    scans.head
  }

  test(s"V2 test - basic variant field extraction ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      // Use V1 to write Parquet files with actual variant data
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, s string) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1, \"b\": 2.5}'), 'test1'), " +
          "(parse_json('{\"a\": 2, \"b\": 3.5}'), 'test2')")
      }

      // Use V2 to read back
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")

        val query = "select variant_get(v, '$.a', 'int') as a, v, " +
          "cast(v as struct<b float>) as v_cast from T_V2"

        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }

        // Validate results are the same with and without pushdown
        checkAnswer(sql(query), expectedRows)

        // Test the variant pushdown
        sql(query).queryExecution.optimizedPlan match {
          case Project(projectList, scanRelation: DataSourceV2ScanRelation) =>
            val output = scanRelation.output
            val v = output(0)
            // Check that variant pushdown happened - v should be a struct, not variant
            assert(v.dataType.isInstanceOf[StructType],
              s"Expected v to be struct type after pushdown, but got ${v.dataType}")
            val vStruct = v.dataType.asInstanceOf[StructType]
            assert(vStruct.fields.length == 3,
              s"Expected 3 fields in struct, got ${vStruct.fields.length}")
            assert(vStruct.fields(0).dataType == IntegerType)
            assert(vStruct.fields(1).dataType == VariantType)
            assert(vStruct.fields(2).dataType.isInstanceOf[StructType])
          case other =>
            fail(s"Expected V2 scan relation with variant pushdown, " +
              s"got ${other.getClass.getName}")
        }
      }
    }
  }

  test(s"V2 test - placeholder field with filter ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values (parse_json('{\"a\": 1}'))")
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")

        val query = "select 1 from T_V2 where isnotnull(v)"

        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }

        // Validate results are the same with and without pushdown
        checkAnswer(sql(query), expectedRows)

        sql(query)
          .queryExecution.optimizedPlan match {
          case Project(_, Filter(condition, scanRelation: DataSourceV2ScanRelation)) =>
            val output = scanRelation.output
            val v = output(0)
            assert(condition == IsNotNull(v))
            assert(v.dataType == StructType(Array(
              field(0, BooleanType, "$.__placeholder_field__", failOnError = false,
                timeZone = "UTC"))))
          case other => fail(s"Expected filtered V2 scan relation, got ${other.getClass.getName}")
        }
      }
    }
  }

  test(s"V2 test - arithmetic and try_variant_get ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1, \"b\": \"hello\"}')), " +
          "(parse_json('{\"a\": 2, \"b\": \"world\"}'))")
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")

        val query = "select variant_get(v, '$.a', 'int') + 1 as a, " +
          "try_variant_get(v, '$.b', 'string') as b from T_V2 " +
          "where variant_get(v, '$.a', 'int') = 1"

        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }

        // Validate results are the same with and without pushdown
        checkAnswer(sql(query), expectedRows)

        sql(query).queryExecution.optimizedPlan match {
          case Project(_, Filter(_, scanRelation: DataSourceV2ScanRelation)) =>
            val output = scanRelation.output
            val v = output(0)
            assert(v.dataType.isInstanceOf[StructType],
              s"Expected v to be struct type, but got ${v.dataType}")
            val vStruct = v.dataType.asInstanceOf[StructType]
            assert(vStruct.fields.length == 2, s"Expected 2 fields in struct")
            assert(vStruct.fields(0).dataType == IntegerType)
            assert(vStruct.fields(1).dataType == StringType)
          case other => fail(s"Expected filtered V2 scan relation, got ${other.getClass.getName}")
        }
      }
    }
  }

  test(s"V2 test - nested variant in struct ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("temp_v1") {
        sql(s"create table temp_v1 (vs struct<v1 variant, v2 variant, i int>) " +
          s"using PARQUET location '$path'")
        sql("insert into temp_v1 select named_struct('v1', parse_json('{\"a\": 1, \"b\": 2}'), " +
          "'v2', parse_json('{\"a\": 3}'), 'i', 100)")
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")

        val query = "select variant_get(vs.v1, '$.a', 'int') as a, " +
          "variant_get(vs.v1, '$.b', 'int') as b, " +
          "variant_get(vs.v2, '$.a', 'int') as a2, vs.i from T_V2"

        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }

        // Validate results are the same with and without pushdown
        checkAnswer(sql(query), expectedRows)

        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            val output = scanRelation.output
            val vs = output(0)
            assert(vs.dataType.isInstanceOf[StructType])
            val vsStruct = vs.dataType.asInstanceOf[StructType]
            // Should have 3 fields: v1 (struct), v2 (struct), i (int)
            assert(vsStruct.fields.length == 3, s"Expected 3 fields in vs")
          case other => fail(s"Expected V2 scan relation, got ${other.getClass.getName}")
        }
      }
    }
  }

  test(s"V2 test - nested variant extraction in aggregate function arg is hoisted and shredded " +
      s"($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (vs struct<v1 variant, i int>, name string) " +
          s"using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(named_struct('v1', parse_json('{\"price\": 10}'), 'i', 1), 'a'), " +
          "(named_struct('v1', parse_json('{\"price\": 30}'), 'i', 2), 'a'), " +
          "(named_struct('v1', parse_json('{\"price\": 20}'), 'i', 3), 'b')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // The extraction is on a variant nested in a struct (vs.v1). Its child is a GetStructField
        // chain rooted at an attribute, which isVariantColumnPath accepts, so it is hoisted and the
        // nested variant shreds to the $.price slot.
        val query =
          "select name, max(variant_get(vs.v1, '$.price', 'int')) as mx from T_V2 group by name"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows) // a -> 30, b -> 20
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vsStruct = scanRelation.output.find(_.name == "vs").get.dataType match {
          case s: StructType => s
          case other => fail(s"Expected vs to stay a struct, but got $other")
        }
        val v1Struct = vsStruct.fields.find(_.name == "v1").get.dataType match {
          case s: StructType => s
          case other => fail(s"Expected nested v1 shredded to struct, but got $other")
        }
        assert(!v1Struct.fields.exists(_.dataType.isInstanceOf[VariantType]),
          s"Expected no full-variant slot in nested v1, but got $v1Struct")
      }
    }
  }

  test(s"V2 test - cast(variant) in aggregate function arg is hoisted and shredded ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, name string) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('\"10\"'), 'a'), (parse_json('\"30\"'), 'a'), (parse_json('\"20\"'), 'b')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // The aggregate argument is `cast(v as string)`, not variant_get. isHoistable's Cast branch
        // (matching collectRequestedFields) makes it eligible, so v shreds to the cast slot.
        val query = "select name, max(cast(v as string)) as mx from T_V2 group by name"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vStruct = scanRelation.output.find(_.name == "v").get.dataType match {
          case s: StructType => s
          case other => fail(s"Expected v shredded to struct, but got $other")
        }
        assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
          s"Expected no full-variant slot, but got $vStruct")
      }
    }
  }

  test(s"V2 test - no pushdown when struct is used ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("temp_v1") {
        sql(s"create table temp_v1 (vs struct<v1 variant, v2 variant, i int>) " +
          s"using PARQUET location '$path'")
        sql("insert into temp_v1 select named_struct('v1', parse_json('{\"a\": 1}'), " +
          "'v2', parse_json('{\"a\": 2}'), 'i', 100)")
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")

        val query = "select vs, variant_get(vs.v1, '$.a', 'int') as a from T_V2"

        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }

        // Validate results are the same with and without pushdown
        checkAnswer(sql(query), expectedRows)

        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            val output = scanRelation.output
            val vs = output(0)
            assert(vs.dataType.isInstanceOf[StructType])
            val vsStruct = vs.dataType.asInstanceOf[StructType]
            // When struct is used directly, variants inside should NOT be pushed down
            val v1Field = vsStruct.fields.find(_.name == "v1").get
            assert(v1Field.dataType == VariantType,
              s"Expected v1 to remain VariantType, but got ${v1Field.dataType}")
          case other => fail(s"Expected V2 scan relation, got ${other.getClass.getName}")
        }
      }
    }
  }

  test(s"V2 test - no pushdown for variant in array ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("temp_v1") {
        sql(s"create table temp_v1 (va array<variant>) using PARQUET location '$path'")
        sql("insert into temp_v1 select array(parse_json('{\"a\": 1}'))")
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")

        val query = "select variant_get(va[0], '$.a', 'int') as a from T_V2"

        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }

        // Validate results are the same with and without pushdown
        checkAnswer(sql(query), expectedRows)

        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            val output = scanRelation.output
            val va = output(0)
            assert(va.dataType.isInstanceOf[ArrayType])
            val arrayType = va.dataType.asInstanceOf[ArrayType]
            assert(arrayType.elementType == VariantType,
              s"Expected array element to be VariantType, but got ${arrayType.elementType}")
          case other => fail(s"Expected V2 scan relation, got ${other.getClass.getName}")
        }
      }
    }
  }

  test(s"V2 test - no pushdown for variant with default ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("temp_v1") {
        sql(s"create table temp_v1 (vd variant default parse_json('1')) " +
          s"using PARQUET location '$path'")
        sql("insert into temp_v1 select parse_json('{\"a\": 1}')")
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")

        val query = "select variant_get(vd, '$.a', 'int') as a from T_V2"

        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }

        // Validate results are the same with and without pushdown
        checkAnswer(sql(query), expectedRows)

        sql(query)
          .queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            val output = scanRelation.output
            val vd = output(0)
            assert(vd.dataType == VariantType,
              s"Expected vd to remain VariantType, but got ${vd.dataType}")
          case other => fail(s"Expected V2 scan relation, got ${other.getClass.getName}")
        }
      }
    }
  }

  test(s"V2 test - no pushdown for non-literal path ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, s string) using PARQUET location '$path'")
        sql("insert into temp_v1 values (parse_json('{\"a\": 1, \"b\": 2}'), '$.a')")
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")

        val query = "select variant_get(v, '$.a', 'int') as a, " +
          "variant_get(v, s, 'int') as v2, v, " +
          "cast(v as struct<b float>) as v3 from T_V2"

        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }

        // Validate results are the same with and without pushdown
        checkAnswer(sql(query), expectedRows)

        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            val output = scanRelation.output
            val v = output(0)
            assert(v.dataType.isInstanceOf[StructType])
            val vStruct = v.dataType.asInstanceOf[StructType]
            // Should have 3 fields: literal path extraction, full variant, cast
            assert(vStruct.fields.length == 3,
              s"Expected 3 fields in struct, got ${vStruct.fields.length}")
            assert(vStruct.fields(0).dataType == IntegerType)
            assert(vStruct.fields(1).dataType == VariantType)
            assert(vStruct.fields(2).dataType.isInstanceOf[StructType])
          case other => fail(s"Expected V2 scan relation, got ${other.getClass.getName}")
        }
      }
    }
  }

  test(s"V2 test - column pruning: only referenced variant column in scan ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v1 variant, v2 variant, v3 variant) using PARQUET " +
          s"location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}'), parse_json('{\"c\": 3}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        val query = "select variant_get(v1, '$.a', 'int') from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            // Only v1 should be in the scan; v2 and v3 must be pruned.
            assert(scanRelation.output.map(_.name) == Seq("v1"),
              s"Expected scan output [v1] but got ${scanRelation.output.map(_.name)}")
            assert(scanRelation.output(0).dataType.isInstanceOf[StructType],
              "Expected v1 to be rewritten to struct type after extraction pushdown")
          case other =>
            fail(s"Unexpected plan shape: ${other.getClass.getName}\n$other")
        }
      }
    }
  }

  test(s"V2 test - column pruning: mixed variant/scalar, only variant referenced ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, s string, i int) using PARQUET location '$path'")
        sql("insert into temp_v1 values (parse_json('{\"a\": 42}'), 'hello', 7)")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        val query = "select variant_get(v, '$.a', 'int') from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            // s and i are unreferenced; only v should appear in the scan.
            assert(scanRelation.output.map(_.name) == Seq("v"),
              s"Expected scan output [v] but got ${scanRelation.output.map(_.name)}")
            // v is referenced via variant_get, so it is rewritten to an extraction struct
            // (slot per path) rather than read as a whole variant.
            assert(scanRelation.output(0).dataType.isInstanceOf[StructType],
              s"Expected v rewritten to struct, but got ${scanRelation.output(0).dataType}")
          case other =>
            fail(s"Unexpected plan shape: ${other.getClass.getName}\n$other")
        }
      }
    }
  }

  test(s"V2 test - column pruning: referenced scalar survives alongside variant ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, s string) using PARQUET location '$path'")
        sql("insert into temp_v1 values (parse_json('{\"a\": 5}'), 'keep_me')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        val query = "select variant_get(v, '$.a', 'int'), s from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            // Both v and s are referenced; neither should be pruned.
            val names = scanRelation.output.map(_.name).toSet
            assert(names == Set("v", "s"),
              s"Expected scan output {v, s} but got $names")
            // v is rewritten to an extraction struct; the scalar s keeps its original type.
            val byName = scanRelation.output.map(a => a.name -> a.dataType).toMap
            assert(byName("v").isInstanceOf[StructType],
              s"Expected v rewritten to struct, but got ${byName("v")}")
            assert(byName("s") == StringType,
              s"Expected s to remain StringType, but got ${byName("s")}")
          case other =>
            fail(s"Unexpected plan shape: ${other.getClass.getName}\n$other")
        }
      }
    }
  }

  test(s"V2 test - column pruning: filter on second variant keeps it in scan ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v1 variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 10}'), parse_json('{\"b\": 1}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        val query =
          "select variant_get(v1, '$.a', 'int') from T_V2 " +
          "where variant_get(v2, '$.b', 'int') > 0"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        // v1 is in SELECT, v2 is in WHERE -- both must survive column pruning.
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val names = scanRelation.output.map(_.name).toSet
        assert(names == Set("v1", "v2"),
          s"Expected scan output {v1, v2} but got $names")
        // Both are referenced via variant_get, so both are rewritten to extraction structs.
        scanRelation.output.foreach { a =>
          assert(a.dataType.isInstanceOf[StructType],
            s"Expected ${a.name} rewritten to struct, but got ${a.dataType}")
        }
      }
    }
  }

  test(s"V2 test - isnotnull on variant with sibling variant column ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v1 variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values (parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        val query = "select 1 from T_V2 where isnotnull(v1)"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        // Only v1 should be in the scan; v2 is unreferenced and must be pruned.
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output.map(_.name) == Seq("v1"),
          s"Expected scan output [v1] but got ${scanRelation.output.map(_.name)}")
        assert(scanRelation.output(0).dataType.isInstanceOf[StructType],
          "Expected v1 to be rewritten to a placeholder struct after pushdown")
      }
    }
  }

  // Aggregate barrier: an aggregate over one variant column (v1) with a filter on the same
  // column. The Aggregate node terminates the PhysicalOperation match, so variant extraction
  // pushdown only fires on the Filter -> Project -> ScanBuilderHolder subtree below it. The
  // $.price path inside max() lives above the barrier and is NOT pushed (v1 stays a full
  // variant), but the unreferenced sibling v2 is never in the subtree's projectList/filters,
  // so top-level column pruning still drops it from the scan.
  test(s"V2 test - column pruning: aggregate barrier prunes unreferenced variant ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v1 variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"price\": 10, \"country\": \"china\"}'), parse_json('{\"b\": 2}')), " +
          "(parse_json('{\"price\": 20, \"country\": \"japan\"}'), parse_json('{\"b\": 3}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        val query =
          "select max(variant_get(v1, '$.price', 'long')) from T_V2 " +
          "where variant_get(v1, '$.country', 'string') = 'china'"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        // v2 is unreferenced anywhere in the query -- it must be pruned out of the scan even
        // though the aggregate barrier prevents the $.price extraction from being pushed.
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output.map(_.name) == Seq("v1"),
          s"Expected scan output [v1] but got ${scanRelation.output.map(_.name)}")
      }
    }
  }

  test(s"V2 test - column pruning: group by + variant_get aggregate, sibling pruned " +
      s"($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant, name string) " +
          s"using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"price\": 10, \"country\": \"china\"}'), " +
          " parse_json('{\"junk\": 1}'), 'widget'), " +
          "(parse_json('{\"price\": 20, \"country\": \"china\"}'), " +
          " parse_json('{\"junk\": 2}'), 'widget'), " +
          "(parse_json('{\"price\": 30, \"country\": \"japan\"}'), " +
          " parse_json('{\"junk\": 3}'), 'gadget')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        val query =
          "select name, max(variant_get(v, '$.price', 'long')) from T_V2 " +
          "where variant_get(v, '$.country', 'string') = 'china' group by name"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        // v2 is unreferenced and must be pruned. v is referenced via variant_get in the WHERE
        // (local, $.country) and via max(variant_get(v, '$.price')) above the aggregate barrier
        // (lifted in as a bare reference -> fullVariant). Because the local $.country extraction is
        // also present, fullVariant is not the column's only field, so v is still shredded (a
        // multi-slot struct does not collapse to a placeholder); it is not kept raw.
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output.map(_.name).toSet == Set("v", "name"),
          s"Expected scan output {v, name} but got ${scanRelation.output.map(_.name)}")
        val vAttr = scanRelation.output.find(_.name == "v").get
        assert(vAttr.dataType.isInstanceOf[StructType],
          s"Expected v rewritten to struct, but got ${vAttr.dataType}")
      }
    }
  }

  test(s"V2 test - order by variant_get: hoisted and shredded, correct ordering, sibling pruned " +
      s"($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant, name string) " +
          s"using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"price\": 3}'), parse_json('{\"junk\": 1}'), 'x'), " +
          "(parse_json('{\"price\": 1}'), parse_json('{\"junk\": 2}'), 'z'), " +
          "(parse_json('{\"price\": 2}'), parse_json('{\"junk\": 3}'), 'y')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // The sort key variant_get(v, '$.price') lives in Sort.order. PullOutVariantExtractions
        // hoists it into a Project below the Sort, and since v is not otherwise referenced above
        // the Sort (only `name` is selected), the raw v is dropped and v shreds to the $.price slot
        // (no full-variant slot). Compare ORDER-SENSITIVELY: a placeholder slot would silently
        // mis-order, which the order-insensitive checkAnswer would not catch.
        val query = "select name from T_V2 order by variant_get(v, '$.price', 'int')"
        val expectedOrder = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect().map(_.getString(0)).toList
        }
        assert(expectedOrder == List("z", "y", "x"),
          s"baseline sanity: expected z,y,x but got $expectedOrder")
        assert(sql(query).collect().map(_.getString(0)).toList == expectedOrder,
          "ORDER BY variant_get produced the wrong row order")
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output.map(_.name).toSet == Set("v", "name"),
          s"Expected scan output {v, name} but got ${scanRelation.output.map(_.name)}")
        val vStruct = scanRelation.output.find(_.name == "v").get.dataType match {
          case s: StructType => s
          case other => fail(s"Expected v shredded to struct, but got $other")
        }
        assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
          s"Expected no full-variant slot, but got $vStruct")
      }
    }
  }

  test(s"V2 test - aggregate max(variant_get) with no local filter: hoisted and shredded " +
      s"($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant, name string) " +
          s"using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"price\": 10}'), parse_json('{\"junk\": 1}'), 'a'), " +
          "(parse_json('{\"price\": 30}'), parse_json('{\"junk\": 2}'), 'a'), " +
          "(parse_json('{\"price\": 20}'), parse_json('{\"junk\": 3}'), 'b')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // variant_get(v, '$.price') is inside an aggregate function, above the aggregate barrier.
        // PullOutVariantExtractions hoists it into a Project directly below the Aggregate, so the
        // pushdown shreds v to just the $.price slot: no full-variant slot (so no boolean
        // placeholder to break codegen), and the raw column is never read.
        val query =
          "select name, max(variant_get(v, '$.price', 'int')) as mx from T_V2 group by name"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows) // a -> 30, b -> 20
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vAttr = scanRelation.output.find(_.name == "v").get
        val vStruct = vAttr.dataType match {
          case s: StructType => s
          case other => fail(s"Expected v shredded to struct, but got $other")
        }
        assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
          s"Expected no full-variant slot, but got $vStruct")
        assert(!scanRelation.output.exists(_.name == "v2"),
          s"Expected v2 pruned but got ${scanRelation.output.map(_.name)}")
      }
    }
  }

  test(s"V2 test - aggregate over the whole variant is left raw, no hoist ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, name string) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"price\": 10}'), 'a'), (parse_json('{\"price\": 20}'), 'b')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // count(v) references the whole variant, not a field extraction. PullOutVariantExtractions
        // has nothing to hoist, and the pushdown keeps v raw (shredding a sole full-variant request
        // saves no I/O).
        val query = "select name, count(v) as c from T_V2 group by name"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vAttr = scanRelation.output.find(_.name == "v").get
        assert(vAttr.dataType.isInstanceOf[VariantType],
          s"Expected v left raw, but got ${vAttr.dataType}")
      }
    }
  }

  test(s"V2 test - GROUP BY key and aggregate function argument both shredded, dedup " +
      s"($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"k\": \"a\", \"price\": 10}')), " +
          "(parse_json('{\"k\": \"a\", \"price\": 30}')), " +
          "(parse_json('{\"k\": \"b\", \"price\": 20}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // $.k is the GROUP BY key (relocated below the Aggregate by PullOutGroupingExpressions);
        // $.price appears twice inside aggregate functions (relocated by the pull-out rule).
        // The two $.price extractions must dedup to a single struct slot, and there must be no
        // full-variant slot.
        val query = "select variant_get(v, '$.k', 'string') as k, " +
          "max(variant_get(v, '$.price', 'int')) as mx, " +
          "min(variant_get(v, '$.price', 'int')) as mn " +
          "from T_V2 group by variant_get(v, '$.k', 'string')"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows) // a -> (30, 10), b -> (20, 20)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vStruct = scanRelation.output.find(_.name == "v").get.dataType match {
          case s: StructType => s
          case other => fail(s"Expected v shredded to struct, but got $other")
        }
        assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
          s"Expected no full-variant slot, but got $vStruct")
        // Exactly two data slots: one for $.k, one shared for the two $.price extractions.
        assert(vStruct.fields.length == 2,
          s"Expected 2 shredded fields ($$.k and a deduped $$.price), but got $vStruct")
      }
    }
  }

  test(s"V2 test - aggregate function arg with a non-literal path is not hoisted ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, p string, name string) " +
          s"using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"price\": 10}'), '$.price', 'a'), " +
          "(parse_json('{\"price\": 20}'), '$.price', 'b')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // The extraction path is a column (non-foldable), which the pushdown cannot handle, so the
        // rule must not hoist it and v is read raw. Guards the `path.foldable` predicate.
        val query = "select name, max(variant_get(v, p, 'int')) as mx from T_V2 group by name"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vAttr = scanRelation.output.find(_.name == "v").get
        assert(vAttr.dataType.isInstanceOf[VariantType],
          s"Expected v left raw for a non-literal path, but got ${vAttr.dataType}")
      }
    }
  }

  test(s"V2 test - join selecting the whole variant keeps v raw ($readerName)") {
    withTempPath { dir =>
      val itemsPath = dir.getCanonicalPath + "/items"
      val countriesPath = dir.getCanonicalPath + "/countries"
      withTable("temp_items", "temp_countries") {
        sql(s"create table temp_items (v variant, name string) using PARQUET location '$itemsPath'")
        sql("insert into temp_items values " +
          "(parse_json('{\"country_code\": \"CN\"}'), 'widget'), " +
          "(parse_json('{\"country_code\": \"JP\"}'), 'gadget')")
        sql(s"create table temp_countries (code string, country_name string) " +
          s"using PARQUET location '$countriesPath'")
        sql("insert into temp_countries values ('CN', 'China'), ('JP', 'Japan')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(itemsPath).createOrReplaceTempView("ITEMS_V2")
        spark.read.parquet(countriesPath).createOrReplaceTempView("COUNTRIES_V2")
        // i.v is selected raw (needed above the Join) AND used in the join key. The hoist must NOT
        // drop it: v shreds with a full-variant slot plus the $.country_code slot. Guards the join
        // liveness computation.
        val query =
          "select i.v, c.country_name from ITEMS_V2 i join COUNTRIES_V2 c " +
          "on variant_get(i.v, '$.country_code', 'string') = c.code"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scans = sql(query).queryExecution.optimizedPlan.collect {
          case s: DataSourceV2ScanRelation => s
        }
        val itemsScan = scans.find(_.output.exists(_.name == "v")).getOrElse(
          fail(s"Could not find the items scan in:\n${sql(query).queryExecution.optimizedPlan}"))
        val vStruct = itemsScan.output.find(_.name == "v").get.dataType match {
          case s: StructType => s
          case other => fail(s"Expected v shredded to struct, but got $other")
        }
        assert(vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
          s"Expected a full-variant slot because i.v is selected raw, but got $vStruct")
      }
    }
  }

  test(s"V2 test - join on variant_get key: hoisted and shredded, sibling pruned " +
      s"($readerName)") {
    withTempPath { dir =>
      val itemsPath = dir.getCanonicalPath + "/items"
      val countriesPath = dir.getCanonicalPath + "/countries"
      withTable("temp_items", "temp_countries") {
        sql(s"create table temp_items (v variant, v2 variant, name string) " +
          s"using PARQUET location '$itemsPath'")
        sql("insert into temp_items values " +
          "(parse_json('{\"country_code\": \"CN\"}'), parse_json('{\"junk\": 1}'), 'widget'), " +
          "(parse_json('{\"country_code\": \"JP\"}'), parse_json('{\"junk\": 2}'), 'gadget')")
        sql(s"create table temp_countries (code string, country_name string) " +
          s"using PARQUET location '$countriesPath'")
        sql("insert into temp_countries values ('CN', 'China'), ('JP', 'Japan')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(itemsPath).createOrReplaceTempView("ITEMS_V2")
        spark.read.parquet(countriesPath).createOrReplaceTempView("COUNTRIES_V2")
        val query =
          "select i.name, c.country_name from ITEMS_V2 i join COUNTRIES_V2 c " +
          "on variant_get(i.v, '$.country_code', 'string') = c.code"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scans = sql(query).queryExecution.optimizedPlan.collect {
          case s: DataSourceV2ScanRelation => s
        }
        val itemsScan = scans.find(_.output.exists(_.name == "v")).getOrElse(
          fail(s"Could not find the items scan in:\n${sql(query).queryExecution.optimizedPlan}"))
        // v is the join key (referenced only via variant_get in the join condition).
        // PullOutVariantExtractions hoists the extraction onto the items side, dropping the raw v
        // so it shreds to just the $.country_code slot (no full-variant slot). v2 is pruned.
        assert(itemsScan.output.map(_.name).toSet == Set("v", "name"),
          s"Expected items scan output {v, name} but got ${itemsScan.output.map(_.name)}")
        val vStruct = itemsScan.output.find(_.name == "v").get.dataType match {
          case s: StructType => s
          case other => fail(s"Expected v shredded to struct, but got $other")
        }
        assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
          s"Expected no full-variant slot, but got $vStruct")
      }
    }
  }

  test(s"V2 test - self join on variant_get key: both sides shredded ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, name string) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"id\": 1}'), 'a'), (parse_json('{\"id\": 2}'), 'b')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // Both sides' join keys are variant_get(_, '$.id'); each is hoisted onto its own side and
        // both variant columns shred to the $.id slot with no full-variant slot.
        val query = "select a.name, b.name from T_V2 a join T_V2 b " +
          "on variant_get(a.v, '$.id', 'int') = variant_get(b.v, '$.id', 'int')"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scans = sql(query).queryExecution.optimizedPlan.collect {
          case s: DataSourceV2ScanRelation => s
        }
        assert(scans.length == 2, s"Expected two scans but got ${scans.length}")
        scans.foreach { s =>
          val vStruct = s.output.find(_.name == "v").get.dataType match {
            case st: StructType => st
            case other => fail(s"Expected v shredded to struct, but got $other")
          }
          assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
            s"Expected no full-variant slot, but got $vStruct")
        }
      }
    }
  }

  test(s"V2 test - join on variant_get key with no narrowing Project: left raw, correct " +
      s"($readerName)") {
    withTempPath { dir =>
      val itemsPath = dir.getCanonicalPath + "/items"
      val countriesPath = dir.getCanonicalPath + "/countries"
      withTable("temp_items", "temp_countries") {
        sql(s"create table temp_items (v variant, name string) using PARQUET location '$itemsPath'")
        sql("insert into temp_items values " +
          "(parse_json('{\"country_code\": \"CN\"}'), 'widget'), " +
          "(parse_json('{\"country_code\": \"JP\"}'), 'gadget')")
        sql(s"create table temp_countries (code string, country_name string) " +
          s"using PARQUET location '$countriesPath'")
        sql("insert into temp_countries values ('CN', 'China'), ('JP', 'Japan')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(itemsPath).createOrReplaceTempView("ITEMS_V2")
        spark.read.parquet(countriesPath).createOrReplaceTempView("COUNTRIES_V2")
        // `select *` leaves the Join without a narrowing Project directly above it, so
        // PullOutVariantExtractions does not hoist the join-key extraction (see class doc: hoisting
        // requires a Project above the barrier to know the live-above set). The plan must still be
        // correct with v read raw -- this locks in the documented fallback behavior.
        val query =
          "select * from ITEMS_V2 i join COUNTRIES_V2 c " +
          "on variant_get(i.v, '$.country_code', 'string') = c.code"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scans = sql(query).queryExecution.optimizedPlan.collect {
          case s: DataSourceV2ScanRelation => s
        }
        val itemsScan = scans.find(_.output.exists(_.name == "v")).getOrElse(
          fail(s"Could not find the items scan in:\n${sql(query).queryExecution.optimizedPlan}"))
        val vAttr = itemsScan.output.find(_.name == "v").get
        assert(vAttr.dataType.isInstanceOf[VariantType],
          s"Expected v left raw with no narrowing Project above the join, " +
            s"but got ${vAttr.dataType}")
      }
    }
  }

  test(s"V2 test - pull-out flag off leaves aggregate function arg raw ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, name string) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"price\": 10}'), 'a'), (parse_json('{\"price\": 20}'), 'b')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "",
          SQLConf.PUSH_VARIANT_INTO_SCAN_PULL_OUT_EXTRACTIONS.key -> "false") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // With the pull-out rule disabled, the aggregate function argument is not hoisted and v is
        // read raw (pushdown still enabled). Confirms the flag gates the rule.
        val query =
          "select name, max(variant_get(v, '$.price', 'int')) as mx from T_V2 group by name"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vAttr = scanRelation.output.find(_.name == "v").get
        assert(vAttr.dataType.isInstanceOf[VariantType],
          s"Expected v left raw with pull-out disabled, but got ${vAttr.dataType}")
      }
    }
  }

  test(s"V2 test - join on variant_get key with local variant filter: no crash ($readerName)") {
    withTempPath { dir =>
      val itemsPath = dir.getCanonicalPath + "/items"
      val countriesPath = dir.getCanonicalPath + "/countries"
      withTable("temp_items", "temp_countries") {
        sql(s"create table temp_items (v variant, v2 variant) " +
          s"using PARQUET location '$itemsPath'")
        sql("insert into temp_items values " +
          "(parse_json('{\"country_code\": \"CN\", \"qty\": 5}'), parse_json('{\"junk\": 1}')), " +
          "(parse_json('{\"country_code\": \"JP\", \"qty\": 0}'), parse_json('{\"junk\": 2}'))")
        sql(s"create table temp_countries (code string, country_name string) " +
          s"using PARQUET location '$countriesPath'")
        sql("insert into temp_countries values ('CN', 'China'), ('JP', 'Japan')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(itemsPath).createOrReplaceTempView("ITEMS_V2")
        spark.read.parquet(countriesPath).createOrReplaceTempView("COUNTRIES_V2")
        // v appears in both a join condition and a local WHERE extraction. The join reference
        // forces v to stay raw; the query must still optimize without crashing and be correct.
        val query =
          "select c.country_name from ITEMS_V2 i join COUNTRIES_V2 c " +
          "on variant_get(i.v, '$.country_code', 'string') = c.code " +
          "where variant_get(i.v, '$.qty', 'int') > 0"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
      }
    }
  }

  test(s"V2 test - join on variant_get key through an aliasing projection: hoisted and shredded " +
      s"($readerName)") {
    withTempPath { dir =>
      val itemsPath = dir.getCanonicalPath + "/items"
      val countriesPath = dir.getCanonicalPath + "/countries"
      withTable("temp_items", "temp_countries") {
        sql(s"create table temp_items (v variant, v2 variant, name string) " +
          s"using PARQUET location '$itemsPath'")
        sql("insert into temp_items values " +
          "(parse_json('{\"country_code\": \"CN\"}'), parse_json('{\"junk\": 1}'), 'widget'), " +
          "(parse_json('{\"country_code\": \"JP\"}'), parse_json('{\"junk\": 2}'), 'gadget')")
        sql(s"create table temp_countries (code string, country_name string) " +
          s"using PARQUET location '$countriesPath'")
        sql("insert into temp_countries values ('CN', 'China'), ('JP', 'Japan')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(itemsPath).createOrReplaceTempView("ITEMS_V2")
        spark.read.parquet(countriesPath).createOrReplaceTempView("COUNTRIES_V2")
        // The variant column is aliased (v AS vw) in a subquery before the join condition reads
        // it via variant_get. The optimizer inlines the alias, PullOutVariantExtractions hoists the
        // extraction onto the items side, and the join key shreds to the $.country_code slot with
        // correct results (checkAnswer) and no wrong data. v2 is pruned.
        val query =
          "select c.country_name from " +
          "(select v as vw, name as nm from ITEMS_V2) i join COUNTRIES_V2 c " +
          "on variant_get(i.vw, '$.country_code', 'string') = c.code"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        // Must not crash during optimization and must be correct.
        checkAnswer(sql(query), expectedRows)
        val scans = sql(query).queryExecution.optimizedPlan.collect {
          case s: DataSourceV2ScanRelation => s
        }
        val itemsScan = scans.find(_.output.exists(a => a.name == "v" || a.name == "vw"))
          .getOrElse(fail(
            s"Could not find the items scan in:\n${sql(query).queryExecution.optimizedPlan}"))
        // v2 is unreferenced and must be pruned.
        assert(!itemsScan.output.exists(_.name == "v2"),
          s"Expected v2 pruned but items scan was ${itemsScan.output.map(_.name)}")
        // The variant join key is hoisted and shredded to the $.country_code slot, no full-variant.
        val keyAttr = itemsScan.output.find(a => a.name == "v" || a.name == "vw").get
        val vStruct = keyAttr.dataType match {
          case s: StructType => s
          case other => fail(s"Expected the variant join key shredded to struct, but got $other")
        }
        assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
          s"Expected no full-variant slot, but got $vStruct")
      }
    }
  }

  // Non-shredded variant: parse_json writes the variant blob without shredded typed_value
  // columns, so the Parquet file contains only metadata+value columns.  The logical plan
  // rewrite (variant_get -> GetStructField) and column pruning both still apply; the Parquet
  // reader handles the non-shredded data at physical read time.
  // All of the tests above already use parse_json (non-shredded) data, so this comment
  // documents that the non-shredded case is fully covered by the existing test suite.

  test(s"V2 test - sole full variant: select v directly stays raw ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}')), " +
          "(parse_json('{\"a\": 9}'), parse_json('{\"b\": 8}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // Selecting the whole variant is the canonical sole-fullVariant case: its only requested
        // field is the entire value, which is not an extraction. v must stay raw (shredding it to a
        // lone full-variant slot would collapse to a boolean placeholder and corrupt the output).
        // v2 is unreferenced and must be pruned.
        val query = "select v from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output.map(_.name) == Seq("v"),
          s"Expected scan output [v] but got ${scanRelation.output.map(_.name)}")
        assert(scanRelation.output(0).dataType.isInstanceOf[VariantType],
          s"Expected v left raw, but got ${scanRelation.output(0).dataType}")
      }
    }
  }

  test(s"V2 test - sole full variant on one column, sibling extracted ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}')), " +
          "(parse_json('{\"a\": 9}'), parse_json('{\"b\": 8}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // The guard is per-column: v is read whole (sole fullVariant -> raw) while v2 has a real
        // extraction (-> shredded). Both must coexist in the same scan.
        val query = "select v, variant_get(v2, '$.b', 'int') as b from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vAttr = scanRelation.output.find(_.name == "v").get
        val v2Attr = scanRelation.output.find(_.name == "v2").get
        assert(vAttr.dataType.isInstanceOf[VariantType],
          s"Expected v left raw, but got ${vAttr.dataType}")
        assert(v2Attr.dataType.isInstanceOf[StructType],
          s"Expected v2 shredded to struct, but got ${v2Attr.dataType}")
      }
    }
  }

  test(s"V2 test - sole 2-arg variant_get (VariantType target) stays raw ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": {\"x\": 1}}'), parse_json('{\"b\": 2}')), " +
          "(parse_json('{\"a\": {\"x\": 9}}'), parse_json('{\"b\": 8}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // 2-arg variant_get has no type argument, so its target is VariantType. Its requested
        // field is NOT the exact `fullVariant` sentinel (the path is "$.a", not "$"), but it still
        // shreds to a lone variant-typed slot which the reader collapses to a boolean placeholder.
        // The strip must catch it by target type, not by exact-value equality, and keep v raw.
        val query = "select variant_get(v, '$.a') as a from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        // v is read whole (kept raw); the unreferenced sibling v2 is pruned.
        assert(scanRelation.output.map(_.name) == Seq("v"),
          s"Expected scan output [v] but got ${scanRelation.output.map(_.name)}")
        assert(scanRelation.output(0).dataType.isInstanceOf[VariantType],
          s"Expected v left raw, but got ${scanRelation.output(0).dataType}")
      }
    }
  }

  test(s"V2 test - sole variant_get('$$') under non-UTC session stays raw ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}')), " +
          "(parse_json('{\"a\": 9}'), parse_json('{\"b\": 8}'))")
      }
      // A non-UTC session makes variant_get carry a non-UTC timezone, so even the "$" path is not
      // equal to the UTC `fullVariant` sentinel. The strip must still keep v raw -- matching by
      // target type rather than by the exact sentinel value covers this case.
      withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> "",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        val query = "select variant_get(v, '$') as whole from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output.map(_.name) == Seq("v"),
          s"Expected scan output [v] but got ${scanRelation.output.map(_.name)}")
        assert(scanRelation.output(0).dataType.isInstanceOf[VariantType],
          s"Expected v left raw, but got ${scanRelation.output(0).dataType}")
      }
    }
  }

  test(s"V2 test - mixed extractions with order by a pushed path: shredded, correct order " +
      s"($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"name\": \"x\", \"price\": 3}'), parse_json('{\"j\": 1}')), " +
          "(parse_json('{\"name\": \"z\", \"price\": 1}'), parse_json('{\"j\": 2}')), " +
          "(parse_json('{\"name\": \"y\", \"price\": 2}'), parse_json('{\"j\": 3}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // Two real extractions ($.name, $.price) are in the local Project, and the ORDER BY lifts a
        // bare v (-> fullVariant) for the sort. fullVariant is not the only field, so v stays
        // shredded (a multi-slot struct with a real full-variant slot, no placeholder collapse) and
        // the sort reads that real slot. Compare ORDER-SENSITIVELY.
        val query = "select variant_get(v, '$.name', 'string') as nm, " +
          "variant_get(v, '$.price', 'long') as pr " +
          "from T_V2 order by variant_get(v, '$.price', 'long')"
        val expectedOrder = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect().map(r => (r.getString(0), r.getLong(1))).toList
        }
        assert(expectedOrder == List(("z", 1L), ("y", 2L), ("x", 3L)),
          s"baseline sanity: $expectedOrder")
        val actualOrder = sql(query).collect().map(r => (r.getString(0), r.getLong(1))).toList
        assert(actualOrder == expectedOrder,
          "ORDER BY a pushed path produced the wrong row order")
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vAttr = scanRelation.output.find(_.name == "v").get
        assert(vAttr.dataType.isInstanceOf[StructType],
          s"Expected v shredded, but got ${vAttr.dataType}")
      }
    }
  }

  test(s"V2 test - order by a pushed path not in the select list: correct order ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"name\": \"x\", \"price\": 3}'), parse_json('{\"j\": 1}')), " +
          "(parse_json('{\"name\": \"z\", \"price\": 1}'), parse_json('{\"j\": 2}')), " +
          "(parse_json('{\"name\": \"y\", \"price\": 2}'), parse_json('{\"j\": 3}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // Only $.name is selected (local); the sort key $.price is lifted as a bare v (full
        // variant). v shreds to {$.name, full-variant}; the sort reads the real full-variant slot.
        // The ORDER-SENSITIVE check guards against a placeholder mis-order.
        val query = "select variant_get(v, '$.name', 'string') as nm " +
          "from T_V2 order by variant_get(v, '$.price', 'long')"
        val expectedOrder = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect().map(_.getString(0)).toList
        }
        assert(expectedOrder == List("z", "y", "x"), s"baseline sanity: $expectedOrder")
        assert(sql(query).collect().map(_.getString(0)).toList == expectedOrder,
          "ORDER BY a non-selected pushed path produced the wrong row order")
      }
    }
  }

  test(s"V2 test - group by variant_get key: optimal shred, no full-variant slot ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"k\": \"a\"}'), parse_json('{\"j\": 1}')), " +
          "(parse_json('{\"k\": \"a\"}'), parse_json('{\"j\": 2}')), " +
          "(parse_json('{\"k\": \"b\"}'), parse_json('{\"j\": 3}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // The grouping expression variant_get(v, '$.k') is pulled into a Project below the
        // Aggregate (PullOutGroupingExpressions), so it is visible to the rewrite and shreds to
        // the $.k slot -- a real extraction, no full-variant slot and no bare-v lift. v2 is pruned.
        val query =
          "select variant_get(v, '$.k', 'string') as k, count(1) as n " +
          "from T_V2 group by variant_get(v, '$.k', 'string')"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output.map(_.name) == Seq("v"),
          s"Expected scan output [v] but got ${scanRelation.output.map(_.name)}")
        val vStruct = scanRelation.output(0).dataType match {
          case s: StructType => s
          case other => fail(s"Expected v shredded to struct, but got $other")
        }
        assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
          s"Expected optimal extraction with no full-variant slot, but got $vStruct")
      }
    }
  }

  test(s"V2 test - distinct variant_get: optimal shred ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"k\": \"a\"}'), parse_json('{\"j\": 1}')), " +
          "(parse_json('{\"k\": \"a\"}'), parse_json('{\"j\": 2}')), " +
          "(parse_json('{\"k\": \"b\"}'), parse_json('{\"j\": 3}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // DISTINCT becomes an Aggregate whose grouping expression is the distinct column; like
        // GROUP BY, the variant_get is pulled below the Aggregate and shreds to the $.k slot.
        val query = "select distinct variant_get(v, '$.k', 'string') as k from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output(0).dataType.isInstanceOf[StructType],
          s"Expected v shredded, but got ${scanRelation.output(0).dataType}")
      }
    }
  }

  test(s"V2 test - window order by variant_get: no crash, shredded ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"k\": \"a\", \"price\": 3}'), parse_json('{\"j\": 1}')), " +
          "(parse_json('{\"k\": \"a\", \"price\": 1}'), parse_json('{\"j\": 2}')), " +
          "(parse_json('{\"k\": \"b\", \"price\": 2}'), parse_json('{\"j\": 3}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // Window is a fourth barrier type. The OVER (ORDER BY variant_get(v, '$.price')) order key
        // and the selected variant_get(v, '$.k') are both pushed; the query must optimize without
        // crashing and return correct results.
        val query = "select variant_get(v, '$.k', 'string') as k, " +
          "row_number() over (order by variant_get(v, '$.price', 'long')) as rn from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output.find(_.name == "v").get.dataType.isInstanceOf[StructType],
          "Expected v shredded for the pushed window extractions")
      }
    }
  }

  test(s"V2 test - two variant columns each extracted: both shredded ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}')), " +
          "(parse_json('{\"a\": 9}'), parse_json('{\"b\": 8}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read.parquet(path).createOrReplaceTempView("T_V2")
        // Both variant columns carry real extractions on different paths; each shreds independently
        // and both remain in the scan.
        val query =
          "select variant_get(v, '$.a', 'int') as a, variant_get(v2, '$.b', 'int') as b from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        assert(scanRelation.output.map(_.name).toSet == Set("v", "v2"),
          s"Expected scan output {v, v2} but got ${scanRelation.output.map(_.name)}")
        assert(scanRelation.output.forall(_.dataType.isInstanceOf[StructType]),
          s"Expected both v and v2 shredded, but got " +
          s"${scanRelation.output.map(a => s"${a.name}:${a.dataType.simpleString}")}")
      }
    }
  }

  test(s"V2 test - column pruning: no variant_get, normal pruning applies ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v1 variant, v2 variant, s string) using PARQUET " +
          s"location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}'), 'hello')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        // No variant_get: pushdown does not fire; normal pruneColumns applies.
        val query = "select s from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        sql(query).queryExecution.optimizedPlan match {
          case scanRelation: DataSourceV2ScanRelation =>
            assert(scanRelation.output.map(_.name) == Seq("s"),
              s"Expected scan output [s] but got ${scanRelation.output.map(_.name)}")
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            assert(scanRelation.output.map(_.name) == Seq("s"),
              s"Expected scan output [s] but got ${scanRelation.output.map(_.name)}")
          case other =>
            fail(s"Unexpected plan shape: ${other.getClass.getName}\n$other")
        }
      }
    }
  }

  test(s"V2 test - column pruning: 2 variant cols, only second referenced ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v1 variant, v2 variant) using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 99}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        val query = "select variant_get(v2, '$.b', 'int') from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            assert(scanRelation.output.map(_.name) == Seq("v2"),
              s"Expected scan output [v2] but got ${scanRelation.output.map(_.name)}")
            assert(scanRelation.output(0).dataType.isInstanceOf[StructType],
              s"Expected v2 rewritten to struct, but got ${scanRelation.output(0).dataType}")
          case other =>
            fail(s"Unexpected plan shape: ${other.getClass.getName}\n$other")
        }
      }
    }
  }

  test(s"V2 test - column pruning: 3 variant cols, 2 referenced ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v1 variant, v2 variant, v3 variant) using PARQUET " +
          s"location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}'), parse_json('{\"c\": 3}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        val query = "select variant_get(v1, '$.a', 'int'), variant_get(v3, '$.c', 'int') from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            val names = scanRelation.output.map(_.name).toSet
            assert(names == Set("v1", "v3"),
              s"Expected scan output {v1, v3} but got $names")
            scanRelation.output.foreach { a =>
              assert(a.dataType.isInstanceOf[StructType],
                s"Expected ${a.name} rewritten to struct, but got ${a.dataType}")
            }
          case other =>
            fail(s"Unexpected plan shape: ${other.getClass.getName}\n$other")
        }
      }
    }
  }

  test(s"V2 test - column pruning: 3 variant cols, all 3 referenced ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v1 variant, v2 variant, v3 variant) using PARQUET " +
          s"location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}'), parse_json('{\"c\": 3}'))")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        val query = "select variant_get(v1, '$.a', 'int'), variant_get(v2, '$.b', 'int'), " +
          "variant_get(v3, '$.c', 'int') from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        sql(query).queryExecution.optimizedPlan match {
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            val names = scanRelation.output.map(_.name).toSet
            assert(names == Set("v1", "v2", "v3"),
              s"Expected all 3 columns in scan but got $names")
            scanRelation.output.foreach { a =>
              assert(a.dataType.isInstanceOf[StructType],
                s"Expected ${a.name} rewritten to struct, but got ${a.dataType}")
            }
          case other =>
            fail(s"Unexpected plan shape: ${other.getClass.getName}\n$other")
        }
      }
    }
  }

  test(s"V2 test - column pruning: 3 variant cols, none referenced via variant_get ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withTable("temp_v1") {
        sql(s"create table temp_v1 (v1 variant, v2 variant, v3 variant, s string) " +
          s"using PARQUET location '$path'")
        sql("insert into temp_v1 values " +
          "(parse_json('{\"a\": 1}'), parse_json('{\"b\": 2}'), parse_json('{\"c\": 3}'), 'hi')")
      }
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.parquet(path)
        df.createOrReplaceTempView("T_V2")
        // Only s is selected; no variant_get, so extraction pushdown does not fire.
        // Normal pruneColumns removes v1/v2/v3.
        val query = "select s from T_V2"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows)
        sql(query).queryExecution.optimizedPlan match {
          case scanRelation: DataSourceV2ScanRelation =>
            assert(scanRelation.output.map(_.name) == Seq("s"),
              s"Expected scan output [s] but got ${scanRelation.output.map(_.name)}")
          case Project(_, scanRelation: DataSourceV2ScanRelation) =>
            assert(scanRelation.output.map(_.name) == Seq("s"),
              s"Expected scan output [s] but got ${scanRelation.output.map(_.name)}")
          case other =>
            fail(s"Unexpected plan shape: ${other.getClass.getName}\n$other")
        }
      }
    }
  }

  test(s"Spark native managed table: aggregate hoist shreds without full-variant slot " +
      s"($readerName)") {
    // A Spark native (managed catalog) Parquet table created with CREATE TABLE ... USING PARQUET
    // resolves to a V1 `LogicalRelation` regardless of USE_V1_SOURCE_LIST -- that config only
    // affects the `spark.read`/DataSource resolution of external file reads, not managed catalog
    // tables. PullOutVariantExtractions must still hoist the aggregate-function extraction just
    // like it does for external-file reads, and the pushdown must shred v to the $.price slot
    // with no full-variant slot. `scanRelationOutput` handles whichever scan node the plan uses.
    withTable("T_native") {
      sql("create table T_native (v variant, name string) using parquet")
      sql("insert into T_native values " +
        "(parse_json('{\"price\": 10}'), 'a'), " +
        "(parse_json('{\"price\": 30}'), 'a'), " +
        "(parse_json('{\"price\": 20}'), 'b')")
      val query =
        "select name, max(variant_get(v, '$.price', 'int')) as mx from T_native group by name"
      val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
        sql(query).collect()
      }
      checkAnswer(sql(query), expectedRows) // a -> 30, b -> 20
      val output = scanRelationOutput(sql(query).queryExecution.optimizedPlan)
      val vStruct = output.find(_.name == "v").get.dataType match {
        case s: StructType => s
        case other => fail(s"Expected v shredded to struct, but got $other")
      }
      assert(!vStruct.fields.exists(_.dataType.isInstanceOf[VariantType]),
        s"Expected no full-variant slot for a native managed table, but got $vStruct")
    }
  }

  test(s"V2 No push down for JSON ($readerName)") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      // Use V1 to write JSON files with variant data
      withTable("temp_v1_json") {
        sql(s"create table temp_v1_json (v variant) using JSON location '$path'")
        sql("insert into temp_v1_json values (parse_json('{\"a\": 1}'))")
      }

      // Use V2 to read back
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        val df = spark.read.format("json").load(path)
        df.createOrReplaceTempView("T_V2_JSON")

        val query = "select v from T_V2_JSON"

        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }

        // Validate results are the same with and without pushdown
        checkAnswer(sql(query), expectedRows)

        // JSON V2 reader performs schema inference - it won't preserve variant type
        // It will infer the schema as a typed struct instead
        sql(query).queryExecution.optimizedPlan match {
          case scanRelation: DataSourceV2ScanRelation =>
            val output = scanRelation.output
            // JSON format with V2 infers schema, so variant becomes a typed struct
            assert(output(0).dataType != VariantType,
              s"Expected non-variant type for JSON V2 due to schema inference, " +
              s"got ${output(0).dataType}")
          case other =>
            fail(s"Expected V2 scan relation, got ${other.getClass.getName}")
        }
      }
    }
  }
}

// V2 DataSource tests - Row-based reader
class PushVariantIntoScanV2Suite extends PushVariantIntoScanV2SuiteBase {
  override protected def vectorizedReaderEnabled: Boolean = false
  override protected def readerName: String = "row-based reader"
}

// V2 DataSource tests - Vectorized reader
class PushVariantIntoScanV2VectorizedSuite extends PushVariantIntoScanV2SuiteBase {
  override protected def vectorizedReaderEnabled: Boolean = true
  override protected def readerName: String = "vectorized reader"
}
