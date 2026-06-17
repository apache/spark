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

  test(s"V2 test - order by variant_get: correct ordering, variant read raw, sibling pruned " +
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
        // The sort key variant_get(v, '$.price') lives in Sort.order, above the scan window, so v
        // is lifted in only as a bare reference -> a whole-variant request -> v stays raw and the
        // sort evaluates on the real variant. Compare ORDER-SENSITIVELY: a shredded full-variant
        // slot would collapse to a boolean placeholder and silently mis-order, which the
        // order-insensitive checkAnswer would not catch.
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
        val vAttr = scanRelation.output.find(_.name == "v").get
        assert(vAttr.dataType.isInstanceOf[VariantType],
          s"Expected v left as raw VariantType, but got ${vAttr.dataType}")
      }
    }
  }

  test(s"V2 test - aggregate max(variant_get) with no local filter: no codegen crash, variant " +
      s"read raw ($readerName)") {
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
        // variant_get(v, '$.price') is inside an aggregate function, above the aggregate barrier,
        // with no local filter/projection on v. v is lifted in only as a bare reference -> a
        // whole-variant request. Before the fix this shredded to a lone full-variant slot, which
        // the Parquet reader collapsed to a boolean placeholder; max(variant_get(<boolean>, ...))
        // then failed to codegen. Keeping v raw avoids the crash and yields correct aggregates.
        val query =
          "select name, max(variant_get(v, '$.price', 'int')) as mx from T_V2 group by name"
        val expectedRows = withSQLConf(SQLConf.PUSH_VARIANT_INTO_SCAN.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(sql(query), expectedRows) // a -> 30, b -> 20
        val scanRelation = findScanRelation(sql(query).queryExecution.optimizedPlan)
        val vAttr = scanRelation.output.find(_.name == "v").get
        assert(vAttr.dataType.isInstanceOf[VariantType],
          s"Expected v left as raw VariantType, but got ${vAttr.dataType}")
        assert(!scanRelation.output.exists(_.name == "v2"),
          s"Expected v2 pruned but got ${scanRelation.output.map(_.name)}")
      }
    }
  }

  test(s"V2 test - join on variant_get key: no crash, variant read raw, sibling pruned " +
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
        // Must not crash during optimization (SPARK-57499 regression) and must be correct.
        checkAnswer(sql(query), expectedRows)
        val scans = sql(query).queryExecution.optimizedPlan.collect {
          case s: DataSourceV2ScanRelation => s
        }
        val itemsScan = scans.find(_.output.exists(_.name == "v")).getOrElse(
          fail(s"Could not find the items scan in:\n${sql(query).queryExecution.optimizedPlan}"))
        // v is the join key (referenced only via variant_get in the join condition): it must be
        // left as a raw variant, not shredded. v2 is unreferenced and must be pruned.
        assert(itemsScan.output.map(_.name).toSet == Set("v", "name"),
          s"Expected items scan output {v, name} but got ${itemsScan.output.map(_.name)}")
        val vAttr = itemsScan.output.find(_.name == "v").get
        assert(vAttr.dataType.isInstanceOf[VariantType],
          s"Expected v left as raw VariantType, but got ${vAttr.dataType}")
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

  test(s"V2 test - join on variant_get key through an aliasing projection: no crash, no " +
      s"mis-shred ($readerName)") {
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
        // it via variant_get. Whether the optimizer inlines the alias or keeps it, the query must
        // optimize without crashing, return correct results, and not over-shred (v2 pruned, the
        // variant join key not turned into a struct that yields wrong data).
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
        // The variant join key must not be shredded: it is read as a raw variant.
        val vAttr = itemsScan.output.find(a => a.name == "v" || a.name == "vw").get
        assert(vAttr.dataType.isInstanceOf[VariantType],
          s"Expected the variant join key left raw, but got ${vAttr.dataType}")
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
