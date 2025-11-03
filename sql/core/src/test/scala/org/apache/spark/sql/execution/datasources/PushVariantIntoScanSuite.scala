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

}

// V1 DataSource tests with parameterized reader type
abstract class PushVariantIntoScanV1SuiteBase extends PushVariantIntoScanSuiteBase {
  protected def vectorizedReaderEnabled: Boolean
  protected def readerName: String

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
