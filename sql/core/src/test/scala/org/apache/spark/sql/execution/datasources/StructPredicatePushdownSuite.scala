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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, GetStructField, Literal}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for struct equality predicate decomposition at the pushdown layer.
 *
 * Validates that:
 * - Decomposed field-level predicates reach the scan (PushedFilters/DataFilters)
 * - The original struct predicate is retained as a post-scan filter (correctness)
 * - Null-valued literal fields are NOT pushed as `= null` (soundness)
 * - maxFields bound is respected
 * - Conf on/off behavior works correctly
 * - Results are identical with rule on vs off (parity)
 */
class StructPredicatePushdownSuite extends QueryTest with SharedSparkSession {

  private def getFileSourceScanDataFilters(df: DataFrame): Seq[Expression] = {
    val plan = df.queryExecution.executedPlan
    plan.collect { case scan: FileSourceScanExec => scan.dataFilters }.flatten
  }

  private def hasPostScanFilter(df: DataFrame): Boolean = {
    val plan = df.queryExecution.executedPlan
    plan.collect { case f: FilterExec => f }.nonEmpty
  }

  test("field-level predicates are pushed for struct equality") {
    withTempPath { path =>
      spark.range(10).selectExpr(
        "named_struct('a', cast(id as int), 'b', cast(id + 1 as string)) as s"
      ).write.parquet(path.getAbsolutePath)

      val df = spark.read.parquet(path.getAbsolutePath)
        .where("s = named_struct('a', 5, 'b', '6')")

      val dataFilters = getFileSourceScanDataFilters(df)
      // Should contain field-level EqualTo predicates for s.a and s.b
      val fieldPredicates = dataFilters.collect {
        case eq @ EqualTo(GetStructField(_, _, _), Literal(_, _)) => eq
        case eq @ EqualTo(Literal(_, _), GetStructField(_, _, _)) => eq
      }
      assert(fieldPredicates.nonEmpty,
        s"Expected field-level predicates in DataFilters but got: $dataFilters")

      // Verify results are correct
      checkAnswer(df, spark.range(10).selectExpr(
        "named_struct('a', cast(id as int), 'b', cast(id + 1 as string)) as s"
      ).where("s.a = 5 AND s.b = '6'"))
    }
  }

  test("original struct filter is retained post-scan") {
    withTempPath { path =>
      spark.range(10).selectExpr(
        "named_struct('a', cast(id as int), 'b', cast(id + 1 as int)) as s"
      ).write.parquet(path.getAbsolutePath)

      val df = spark.read.parquet(path.getAbsolutePath)
        .where("s = named_struct('a', 3, 'b', 4)")

      // The FilterExec above the scan should still contain the original struct predicate
      assert(hasPostScanFilter(df),
        "Expected post-scan FilterExec containing the original struct predicate")

      checkAnswer(df, Row(Row(3, 4)))
    }
  }

  test("null-valued literal fields are NOT pushed as field = null (soundness)") {
    withTempPath { path =>
      // Create data with some null struct fields
      val data = Seq(
        Row(Row(1, null)),
        Row(Row(2, "hello")),
        Row(null),
        Row(Row(1, "world"))
      )
      val schema = StructType(Seq(
        StructField("s", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", StringType, nullable = true)
        )), nullable = true)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.parquet(path.getAbsolutePath)

      // Filter where the literal has a null field: s = struct(1, null)
      val df = spark.read.parquet(path.getAbsolutePath)
        .where("s = named_struct('a', 1, 'b', cast(null as string))")

      val dataFilters = getFileSourceScanDataFilters(df)
      // Should push s.a = 1 but NOT s.b = null
      val fieldPredicates = dataFilters.collect {
        case eq @ EqualTo(GetStructField(_, ordinal, _), lit: Literal) => (ordinal, lit)
        case eq @ EqualTo(lit: Literal, GetStructField(_, ordinal, _)) => (ordinal, lit)
      }
      // s.a = 1 should be pushed
      assert(fieldPredicates.exists { case (_, lit) => lit.value != null },
        s"Expected non-null field predicates but got: $fieldPredicates")
      // s.b = null should NOT be pushed
      assert(!fieldPredicates.exists { case (_, lit) => lit.value == null },
        s"Should not push null field predicate but found one in: $fieldPredicates")

      // Result correctness: s = struct(1, null) matches the row [1, null] because
      // Spark's struct equality treats null=null field-by-field as equal.
      checkAnswer(df, Row(Row(1, null)))
    }
  }

  test("only non-null literal fields are pushed for struct with all-null fields") {
    withTempPath { path =>
      val data = Seq(Row(Row(null, null)), Row(Row(1, 2)), Row(null))
      val schema = StructType(Seq(
        StructField("s", StructType(Seq(
          StructField("a", IntegerType, nullable = true),
          StructField("b", IntegerType, nullable = true)
        )), nullable = true)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.parquet(path.getAbsolutePath)

      // All fields in literal are null - no field predicates should be generated
      val df = spark.read.parquet(path.getAbsolutePath)
        .where("s = named_struct('a', cast(null as int), 'b', cast(null as int))")

      val dataFilters = getFileSourceScanDataFilters(df)
      val fieldPredicates = dataFilters.collect {
        case eq @ EqualTo(GetStructField(_, _, _), _) => eq
        case eq @ EqualTo(_, GetStructField(_, _, _)) => eq
      }
      // No field predicates should be pushed since all literal fields are null
      assert(fieldPredicates.isEmpty,
        s"Expected no field predicates for all-null literal but got: $fieldPredicates")
    }
  }

  test("EqualNullSafe (<=>) also triggers struct decomposition") {
    withTempPath { path =>
      spark.range(10).selectExpr(
        "named_struct('a', cast(id as int), 'b', cast(id * 2 as int)) as s"
      ).write.parquet(path.getAbsolutePath)

      val df = spark.read.parquet(path.getAbsolutePath)
        .where("s <=> named_struct('a', 3, 'b', 6)")

      val dataFilters = getFileSourceScanDataFilters(df)
      val fieldPredicates = dataFilters.collect {
        case eq @ EqualTo(GetStructField(_, _, _), _) => eq
        case eq @ EqualTo(_, GetStructField(_, _, _)) => eq
      }
      assert(fieldPredicates.nonEmpty,
        s"Expected field-level predicates for <=> but got: $dataFilters")

      checkAnswer(df, Row(Row(3, 6)))
    }
  }

  test("maxFields bound is respected - wide struct is not decomposed") {
    withTempPath { path =>
      // Create a struct with 5 fields
      spark.range(5).selectExpr(
        "named_struct('f1', cast(id as int), 'f2', cast(id as int), " +
          "'f3', cast(id as int), 'f4', cast(id as int), 'f5', cast(id as int)) as s"
      ).write.parquet(path.getAbsolutePath)

      // Set maxFields to 3 so a 5-field struct is not decomposed
      withSQLConf(SQLConf.STRUCT_PREDICATE_DECOMPOSE_MAX_FIELDS.key -> "3") {
        val df = spark.read.parquet(path.getAbsolutePath)
          .where("s = named_struct('f1', 2, 'f2', 2, 'f3', 2, 'f4', 2, 'f5', 2)")

        val dataFilters = getFileSourceScanDataFilters(df)
        val fieldPredicates = dataFilters.collect {
          case eq @ EqualTo(GetStructField(_, _, _), _) => eq
          case eq @ EqualTo(_, GetStructField(_, _, _)) => eq
        }
        // Should NOT have field-level predicates since struct exceeds maxFields
        assert(fieldPredicates.isEmpty,
          s"Expected no field predicates (maxFields=3) but got: $fieldPredicates")
      }

      // With default maxFields (100), the 5-field struct IS decomposed
      val df = spark.read.parquet(path.getAbsolutePath)
        .where("s = named_struct('f1', 2, 'f2', 2, 'f3', 2, 'f4', 2, 'f5', 2)")

      val dataFilters = getFileSourceScanDataFilters(df)
      val fieldPredicates = dataFilters.collect {
        case eq @ EqualTo(GetStructField(_, _, _), _) => eq
        case eq @ EqualTo(_, GetStructField(_, _, _)) => eq
      }
      assert(fieldPredicates.length == 5,
        s"Expected 5 field predicates but got: $fieldPredicates")

      checkAnswer(df, Row(Row(2, 2, 2, 2, 2)))
    }
  }

  test("conf disabled - no decomposition") {
    withTempPath { path =>
      spark.range(10).selectExpr(
        "named_struct('a', cast(id as int), 'b', cast(id + 1 as int)) as s"
      ).write.parquet(path.getAbsolutePath)

      withSQLConf(SQLConf.STRUCT_PREDICATE_DECOMPOSE_ENABLED.key -> "false") {
        val df = spark.read.parquet(path.getAbsolutePath)
          .where("s = named_struct('a', 5, 'b', 6)")

        val dataFilters = getFileSourceScanDataFilters(df)
        val fieldPredicates = dataFilters.collect {
          case eq @ EqualTo(GetStructField(_, _, _), _) => eq
          case eq @ EqualTo(_, GetStructField(_, _, _)) => eq
        }
        // No field-level predicates when conf is disabled
        assert(fieldPredicates.isEmpty,
          s"Expected no field predicates when conf disabled but got: $fieldPredicates")

        // Results should still be correct (just without the pushdown optimization)
        checkAnswer(df, Row(Row(5, 6)))
      }
    }
  }

  test("nested struct decomposition") {
    withTempPath { path =>
      spark.range(10).selectExpr(
        "named_struct('inner', named_struct('x', cast(id as int), " +
          "'y', cast(id + 1 as int)), 'z', cast(id * 10 as int)) as s"
      ).write.parquet(path.getAbsolutePath)

      val df = spark.read.parquet(path.getAbsolutePath)
        .where("s = named_struct('inner', named_struct('x', 3, 'y', 4), 'z', 30)")

      val dataFilters = getFileSourceScanDataFilters(df)
      val fieldPredicates = dataFilters.collect {
        case eq @ EqualTo(g: GetStructField, _: Literal) => eq
        case eq @ EqualTo(_: Literal, g: GetStructField) => eq
      }
      // Should have predicates for s.inner.x, s.inner.y, and s.z (3 leaf fields)
      assert(fieldPredicates.length == 3,
        s"Expected 3 field predicates but got ${fieldPredicates.length}: $dataFilters")

      checkAnswer(df, Row(Row(Row(3, 4), 30)))
    }
  }

  test("parity: results identical with decomposition on vs off") {
    withTempPath { path =>
      // Include rows with: normal values, null struct, struct with null fields
      val data = Seq(
        Row(Row(1, "a")),
        Row(Row(2, "b")),
        Row(Row(1, null)),
        Row(null),
        Row(Row(3, "a"))
      )
      val schema = StructType(Seq(
        StructField("s", StructType(Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", StringType, nullable = true)
        )), nullable = true)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.parquet(path.getAbsolutePath)

      val queries = Seq(
        "s = named_struct('a', 1, 'b', 'a')",
        "s = named_struct('a', 1, 'b', cast(null as string))",
        "s <=> named_struct('a', 1, 'b', 'a')",
        "s <=> named_struct('a', 1, 'b', cast(null as string))"
      )

      for (query <- queries) {
        val resultOn = withSQLConf(
          SQLConf.STRUCT_PREDICATE_DECOMPOSE_ENABLED.key -> "true") {
          spark.read.parquet(path.getAbsolutePath).where(query).collect()
        }
        val resultOff = withSQLConf(
          SQLConf.STRUCT_PREDICATE_DECOMPOSE_ENABLED.key -> "false") {
          spark.read.parquet(path.getAbsolutePath).where(query).collect()
        }
        assert(resultOn.toSeq.sortBy(_.toString) == resultOff.toSeq.sortBy(_.toString),
          s"Results differ for query '$query': on=$resultOn, off=$resultOff")
      }
    }
  }

  test("whole-null struct rows are correctly filtered out") {
    withTempPath { path =>
      val data = Seq(
        Row(Row(1, 2)),
        Row(null),
        Row(Row(3, 4))
      )
      val schema = StructType(Seq(
        StructField("s", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType)
        )), nullable = true)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.parquet(path.getAbsolutePath)

      val df = spark.read.parquet(path.getAbsolutePath)
        .where("s = named_struct('a', 1, 'b', 2)")

      // Post-scan filter ensures null struct rows are filtered out
      checkAnswer(df, Row(Row(1, 2)))
    }
  }

  test("literal on left side of equality") {
    withTempPath { path =>
      spark.range(10).selectExpr(
        "named_struct('a', cast(id as int), 'b', cast(id + 1 as int)) as s"
      ).write.parquet(path.getAbsolutePath)

      // Literal on the left side
      val df = spark.read.parquet(path.getAbsolutePath)
        .where("named_struct('a', 5, 'b', 6) = s")

      val dataFilters = getFileSourceScanDataFilters(df)
      val fieldPredicates = dataFilters.collect {
        case eq @ EqualTo(GetStructField(_, _, _), _) => eq
        case eq @ EqualTo(_, GetStructField(_, _, _)) => eq
      }
      assert(fieldPredicates.nonEmpty,
        s"Expected field predicates for literal-on-left but got: $dataFilters")

      checkAnswer(df, Row(Row(5, 6)))
    }
  }
}
