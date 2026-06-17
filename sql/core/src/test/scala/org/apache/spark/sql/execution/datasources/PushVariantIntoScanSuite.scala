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
