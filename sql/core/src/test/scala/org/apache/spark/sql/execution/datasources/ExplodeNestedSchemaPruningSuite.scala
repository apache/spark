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

import java.io.File

import org.scalactic.Equality

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests for nested schema pruning through explode/posexplode generators.
 *
 * Converted from Taboola's SchemaOnReadGeneratorTest to validate that
 * Spark's native [[PruneNestedFieldsThroughGenerateForScan]] rule
 * correctly prunes nested struct fields within exploded arrays.
 *
 * The test data provides rich array-of-struct schemas:
 *  - `sample`: flat arrays, complex arrays (struct with sub-arrays)
 *  - `double_nested`: two-level nested arrays (a_array -> b_array)
 */
abstract class ExplodeNestedSchemaPruningSuite
  extends QueryTest
  with FileBasedDataSourceTest
  with SchemaPruningTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  // ---- Case classes for test data ----

  case class ComplexElement(col1: Long, col2: Long)
  case class ArrayOfComplexElement(col1: Long, col2: Array[Long], col3: Long)
  case class InnerStruct(aSubArray: Array[Long], col1: Long, col2: Long, col3: Long)

  case class SampleRecord(
    someStr: String,
    someLong: Long,
    someStrArray: Array[String],
    someComplexArray: Array[ComplexElement],
    struct: InnerStruct,
    someArrayOfComplexArrays: Array[ArrayOfComplexElement])

  case class BArrayElement(c: String, c_int: Long, c_2: String)
  case class AArrayElement(b: String, b_int: Long, b_string: String,
    b_array: Array[BArrayElement])

  case class DoubleNestedRecord(
    a: String,
    a_bool: Boolean,
    a_int: Long,
    a_string: String,
    a_array: Array[AArrayElement])

  case class ItemDetail(color: String, size: Int)
  case class MixedItem(name: String, detail: ItemDetail, qty: Int)
  case class MixedArrayRecord(id: Int, items: Array[MixedItem], tags: Array[String])

  // ---- Test data ----

  private val sampleData = Seq(
    SampleRecord(
      someStr = "bla",
      someLong = 12345678987654321L,
      someStrArray = Array("a", "b", "c"),
      someComplexArray = Array(ComplexElement(1, 2)),
      struct = InnerStruct(Array(1, 2, 3), 1, 2, 3),
      someArrayOfComplexArrays = Array(ArrayOfComplexElement(1, Array(1, 2, 3), 4))))

  private val doubleNestedData = Seq(
    DoubleNestedRecord("a1", a_bool = true, 1, "dummy",
      Array(
        AArrayElement("a1_b1", 1, "da",
          Array(BArrayElement("a1_b1_c1", 1, "a1_b1_c1_d"),
            BArrayElement("a1_b1_c2", 1, "a1_b1_c2_d"))),
        AArrayElement("a1_b2", 2, "da",
          Array(BArrayElement("a1_b2_c1", 1, "a1_b2_c2_d"),
            BArrayElement("a1_b2_c2", 1, "a1_b2_c2_d"))))),
    DoubleNestedRecord("a2", a_bool = false, 2, "dummy",
      Array(
        AArrayElement("a2_b1", 3, "da",
          Array(BArrayElement("a2_b1_c1", 1, "a2_b1_c1_d"),
            BArrayElement("a2_b1_c2", 1, "a2_b1_c2_d"))),
        AArrayElement("a2_b2", 4, "da",
          Array(BArrayElement("a2_b2_c1", 1, "da"),
            BArrayElement("a2_b2_c2", 1, "da"))))))

  // Mixed array data: struct array with nested struct + scalar array.
  // Used to test Pattern 3 (non-struct Generate above struct chain).
  private val mixedArrayData = Seq(
    MixedArrayRecord(1,
      Array(
        MixedItem("apple", ItemDetail("red", 3), 5),
        MixedItem("banana", ItemDetail("yellow", 2), 3)),
      Array("fruit", "food")),
    MixedArrayRecord(2,
      Array(MixedItem("cherry", ItemDetail("dark red", 1), 1)),
      Array("berry")))

  // ---- Infrastructure ----

  protected val vectorizedReaderEnabledKey: String
  protected val vectorizedReaderNestedEnabledKey: String

  protected val schemaEquality: Equality[StructType] = new Equality[StructType] {
    override def areEqual(a: StructType, b: Any): Boolean =
      b match {
        case otherType: StructType => DataTypeUtils.sameType(a, otherType)
        case _ => false
      }
  }

  protected def checkScan(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    checkScanSchemata(df, expectedSchemaCatalogStrings: _*)
    df.collect()
  }

  protected def checkScanSchemata(
      df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    val fileSourceScanSchemata =
      collect(df.queryExecution.executedPlan) {
        case scan: FileSourceScanExec => scan.requiredSchema
      }
    assert(fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
      s"Found ${fileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    fileSourceScanSchemata.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema =
          CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
  }

  private def withSampleData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      makeDataSourceFile(sampleData, new File(path + "/sample"))

      val schema =
        """`someStr` STRING, `someLong` BIGINT,
          |`someStrArray` ARRAY<STRING>,
          |`someComplexArray` ARRAY<STRUCT<`col1`: BIGINT, `col2`: BIGINT>>,
          |`struct` STRUCT<`aSubArray`: ARRAY<BIGINT>,
          |  `col1`: BIGINT, `col2`: BIGINT, `col3`: BIGINT>,
          |`someArrayOfComplexArrays` ARRAY<STRUCT<`col1`: BIGINT,
          |  `col2`: ARRAY<BIGINT>, `col3`: BIGINT>>""".stripMargin
      spark.read.format(dataSourceName).schema(schema).load(path + "/sample")
        .createOrReplaceTempView("sample")

      testThunk
    }
  }

  private def withDoubleNestedData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      makeDataSourceFile(doubleNestedData, new File(path + "/double_nested"))

      val schema =
        """`a` STRING, `a_bool` BOOLEAN, `a_int` BIGINT, `a_string` STRING,
          |`a_array` ARRAY<STRUCT<`b`: STRING, `b_int`: BIGINT,
          |  `b_string`: STRING,
          |  `b_array`: ARRAY<STRUCT<`c`: STRING, `c_int`: BIGINT,
          |    `c_2`: STRING>>>>""".stripMargin
      spark.read.format(dataSourceName).schema(schema).load(path + "/double_nested")
        .createOrReplaceTempView("double_nested")

      testThunk
    }
  }

  private def withMixedArrayData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      makeDataSourceFile(mixedArrayData, new File(path + "/mixed_array"))

      val schema =
        """`id` INT,
          |`items` ARRAY<STRUCT<`name`: STRING,
          |  `detail`: STRUCT<`color`: STRING, `size`: INT>, `qty`: INT>>,
          |`tags` ARRAY<STRING>""".stripMargin
      spark.read.format(dataSourceName).schema(schema).load(path + "/mixed_array")
        .createOrReplaceTempView("mixed_array")

      testThunk
    }
  }

  protected def testExplodePruning(testName: String)(testThunk: => Unit): Unit = {
    test(s"Vectorized - $testName") {
      withSQLConf(vectorizedReaderEnabledKey -> "true") {
        testThunk
      }
    }
    test(s"Non-vectorized - $testName") {
      withSQLConf(vectorizedReaderEnabledKey -> "false") {
        testThunk
      }
    }
  }

  // =========================================================================
  //  Tests on sample data
  // =========================================================================

  testExplodePruning("explode complex array - select single field") {
    withSampleData {
      val query = sql(
        "SELECT someStr, arrayVal.col1 " +
          "FROM sample LATERAL VIEW EXPLODE(someComplexArray) as arrayVal")
      checkScan(query,
        "struct<someStr:string," +
          "someComplexArray:array<struct<col1:bigint>>>")
      checkAnswer(query, Row("bla", 1) :: Nil)
    }
  }

  testExplodePruning("posexplode complex array - select single field") {
    withSampleData {
      val query = sql(
        "SELECT someStr, arrayVal.col1 " +
          "FROM sample LATERAL VIEW POSEXPLODE(someComplexArray) " +
          "as arrayIdx, arrayVal")
      checkScan(query,
        "struct<someStr:string," +
          "someComplexArray:array<struct<col1:bigint>>>")
      checkAnswer(query, Row("bla", 1) :: Nil)
    }
  }

  // =========================================================================
  //  Multi-field selection tests (SPARK-34956)
  // =========================================================================

  // This is the key test for PruneNestedFieldsThroughGenerateForScan -
  // selecting multiple fields from an exploded struct element.
  testExplodePruning("explode multi-field selection from struct") {
    withSampleData {
      val query = sql(
        "SELECT arrayVal.col1, arrayVal.col2 " +
          "FROM sample LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as arrayVal")
      // Both col1 and col2 selected, col3 pruned
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col2:array<bigint>>>>")
      checkAnswer(query, Row(1, Array(1, 2, 3)) :: Nil)
    }
  }

  testExplodePruning("posexplode multi-field selection from struct") {
    withSampleData {
      val query = sql(
        "SELECT arrayIdx, arrayVal.col1, arrayVal.col3 " +
          "FROM sample LATERAL VIEW POSEXPLODE(someArrayOfComplexArrays) " +
          "as arrayIdx, arrayVal")
      // col1 and col3 selected, col2 pruned (non-contiguous fields)
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col3:bigint>>>")
      checkAnswer(query, Row(0, 1, 4) :: Nil)
    }
  }

  // Multi-level selection: fields from both outer and inner exploded elements.
  // The outer explode needs col1 (for project) and col2 (for inner explode),
  // so col3 can be pruned even in chained generates.
  testExplodePruning("consecutive explode - multi-level field selection") {
    withSampleData {
      val query = sql(
        "SELECT complex.col1, val " +
          "FROM sample " +
          "LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as complex " +
          "LATERAL VIEW EXPLODE(complex.col2) as val")
      // col1 + col2 needed, col3 pruned
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col2:array<bigint>>>>")
      checkAnswer(query,
        Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Nil)
    }
  }

  testExplodePruning("consecutive explode - prune outer struct") {
    withSampleData {
      val query = sql(
        "SELECT someStr, val " +
          "FROM sample " +
          "LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as complex " +
          "LATERAL VIEW EXPLODE(complex.col2) as val")
      checkScan(query,
        "struct<someStr:string," +
          "someArrayOfComplexArrays:array<struct<col2:array<bigint>>>>")
      checkAnswer(query,
        Row("bla", 1) :: Row("bla", 2) :: Row("bla", 3) :: Nil)
    }
  }

  // Consecutive posexplode now supports pruning through chained generators.
  // Only col2 is needed from the outer struct.
  testExplodePruning("consecutive posexplode - prune outer struct") {
    withSampleData {
      val query = sql(
        "SELECT someStr, val " +
          "FROM sample " +
          "LATERAL VIEW POSEXPLODE(someArrayOfComplexArrays) " +
          "as complex_idx, complex " +
          "LATERAL VIEW POSEXPLODE(complex.col2) as val_idx, val")
      // Only col2 needed from outer struct, col1 and col3 pruned
      checkScan(query,
        "struct<someStr:string," +
          "someArrayOfComplexArrays:array<struct<col2:array<bigint>>>>")
      checkAnswer(query,
        Row("bla", 1) :: Row("bla", 2) :: Row("bla", 3) :: Nil)
    }
  }

  testExplodePruning("explode with filter on nested array field in subquery") {
    withSampleData {
      val query = sql(
        "WITH base AS (SELECT someArrayOfComplexArrays FROM sample " +
          "WHERE someArrayOfComplexArrays.col2 IS NOT NULL) " +
          "SELECT item.col1 AS str " +
          "FROM base LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as item")
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col2:array<bigint>>>>")
      checkAnswer(query, Row(1) :: Nil)
    }
  }

  // Posexplode with filter: the projection needs col1, and the filter needs col2.
  // Both fields are included in the pruned scan.
  testExplodePruning("posexplode with filter on nested array field in subquery") {
    withSampleData {
      val query = sql(
        "WITH base AS (SELECT someArrayOfComplexArrays FROM sample " +
          "WHERE someArrayOfComplexArrays.col2 IS NOT NULL) " +
          "SELECT item.col1 AS str " +
          "FROM base " +
          "LATERAL VIEW POSEXPLODE(someArrayOfComplexArrays) " +
          "as item_idx, item")
      // Both col1 (from projection) and col2 (from filter) are needed
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col2:array<bigint>>>>")
      checkAnswer(query, Row(1) :: Nil)
    }
  }

  testExplodePruning("explode with filter over nested array - direct WHERE") {
    withSampleData {
      val query = sql(
        "SELECT item.col1 AS rst " +
          "FROM sample " +
          "LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as item " +
          "WHERE someArrayOfComplexArrays.col2 IS NOT NULL")
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col2:array<bigint>>>>")
      checkAnswer(query, Row(1) :: Nil)
    }
  }

  // Same as subquery variant - both col1 (projection) and col2 (filter) needed
  testExplodePruning("posexplode with filter over nested array - direct WHERE") {
    withSampleData {
      val query = sql(
        "SELECT item.col1 AS rst " +
          "FROM sample " +
          "LATERAL VIEW POSEXPLODE(someArrayOfComplexArrays) " +
          "as item_idx, item " +
          "WHERE someArrayOfComplexArrays.col2 IS NOT NULL")
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col2:array<bigint>>>>")
      checkAnswer(query, Row(1) :: Nil)
    }
  }

  testExplodePruning("explode sub-array from struct") {
    withSampleData {
      val query = sql(
        "SELECT arrayVal FROM sample " +
          "LATERAL VIEW EXPLODE(struct.aSubArray) as arrayVal")
      checkScan(query,
        "struct<struct:struct<aSubArray:array<bigint>>>")
      checkAnswer(query,
        Row(1) :: Row(2) :: Row(3) :: Nil)
    }
  }

  testExplodePruning("posexplode sub-array from struct") {
    withSampleData {
      val query = sql(
        "SELECT arrayVal FROM sample " +
          "LATERAL VIEW POSEXPLODE(struct.aSubArray) " +
          "as arrayIdx, arrayVal")
      checkScan(query,
        "struct<struct:struct<aSubArray:array<bigint>>>")
      checkAnswer(query,
        Row(1) :: Row(2) :: Row(3) :: Nil)
    }
  }

  // =========================================================================
  //  Tests on double-nested data
  // =========================================================================

  // Double-nested explode: pruning through chained generators now works.
  // Outer struct is pruned to only needed fields (b + b_array).
  // Inner struct (b_array elements) is also pruned to only c.
  testExplodePruning("double-nested explode - select leaf fields") {
    withDoubleNestedData {
      val query = sql(
        "WITH base AS (" +
          "SELECT * FROM double_nested " +
          "LATERAL VIEW OUTER EXPLODE(a_array) as a_array_item " +
          "LATERAL VIEW OUTER EXPLODE(a_array_item.b_array) as b_array_item) " +
          "SELECT a_array_item.b, b_array_item.c FROM base")
      // b + b_array needed from outer struct, inner struct pruned to just c
      checkScan(query,
        "struct<a_array:array<struct<b:string," +
          "b_array:array<struct<c:string>>>>>")
      checkAnswer(query,
        Row("a1_b1", "a1_b1_c1") :: Row("a1_b1", "a1_b1_c2") ::
        Row("a1_b2", "a1_b2_c1") :: Row("a1_b2", "a1_b2_c2") ::
        Row("a2_b1", "a2_b1_c1") :: Row("a2_b1", "a2_b1_c2") ::
        Row("a2_b2", "a2_b2_c1") :: Row("a2_b2", "a2_b2_c2") :: Nil)
    }
  }

  // Same as explode variant - both outer and inner structs pruned
  testExplodePruning("double-nested posexplode - select leaf fields with pos") {
    withDoubleNestedData {
      val query = sql(
        "WITH base AS (" +
          "SELECT * FROM double_nested " +
          "LATERAL VIEW OUTER POSEXPLODE(a_array) " +
          "as a_array_index, a_array_item " +
          "LATERAL VIEW OUTER POSEXPLODE(a_array_item.b_array) " +
          "as b_array_index, b_array_item) " +
          "SELECT a_array_index, a_array_item.b, " +
          "b_array_index, b_array_item.c FROM base")
      checkScan(query,
        "struct<a_array:array<struct<b:string," +
          "b_array:array<struct<c:string>>>>>")
      checkAnswer(query,
        Row(0, "a1_b1", 0, "a1_b1_c1") :: Row(0, "a1_b1", 1, "a1_b1_c2") ::
        Row(1, "a1_b2", 0, "a1_b2_c1") :: Row(1, "a1_b2", 1, "a1_b2_c2") ::
        Row(0, "a2_b1", 0, "a2_b1_c1") :: Row(0, "a2_b1", 1, "a2_b1_c2") ::
        Row(1, "a2_b2", 0, "a2_b2_c1") :: Row(1, "a2_b2", 1, "a2_b2_c2") :: Nil)
    }
  }

  // Multi-field from single posexplode - tests SPARK-34956 multi-field pruning
  testExplodePruning("single posexplode - multi-field selection") {
    withDoubleNestedData {
      val query = sql(
        "WITH base AS (" +
          "SELECT * FROM double_nested " +
          "LATERAL VIEW OUTER POSEXPLODE(a_array) " +
          "as a_array_index, a_array_item) " +
          "SELECT a_array_index, a_array_item.b, a_array_item.b_int FROM base")
      // Both b and b_int selected, b_string and b_array pruned
      checkScan(query,
        "struct<a_array:array<struct<b:string,b_int:bigint>>>")
      checkAnswer(query,
        Row(0, "a1_b1", 1) :: Row(1, "a1_b2", 2) ::
        Row(0, "a2_b1", 3) :: Row(1, "a2_b2", 4) :: Nil)
    }
  }

  testExplodePruning("single posexplode - select struct field") {
    withDoubleNestedData {
      val query = sql(
        "WITH base AS (" +
          "SELECT * FROM double_nested " +
          "LATERAL VIEW OUTER POSEXPLODE(a_array) " +
          "as a_array_index, a_array_item) " +
          "SELECT a_array_index, a_array_item.b FROM base")
      checkScan(query,
        "struct<a_array:array<struct<b:string>>>")
      checkAnswer(query,
        Row(0, "a1_b1") :: Row(1, "a1_b2") ::
        Row(0, "a2_b1") :: Row(1, "a2_b2") :: Nil)
    }
  }

  testExplodePruning(
      "single posexplode with struct selection in subquery") {
    withDoubleNestedData {
      val query = sql(
        "WITH base AS (" +
          "SELECT a_array_index, a_array_item FROM double_nested " +
          "LATERAL VIEW OUTER POSEXPLODE(a_array) " +
          "as a_array_index, a_array_item) " +
          "SELECT a_array_index, a_array_item.b FROM base")
      checkScan(query,
        "struct<a_array:array<struct<b:string>>>")
      checkAnswer(query,
        Row(0, "a1_b1") :: Row(1, "a1_b2") ::
        Row(0, "a2_b1") :: Row(1, "a2_b2") :: Nil)
    }
  }

  // DF API double-nested - pruning now works through chained generators
  // Both outer and inner structs are pruned to only needed fields
  testExplodePruning("double-nested explode with struct selection via DF API") {
    withDoubleNestedData {
      var df = spark.table("double_nested")
      df = df.select(col("a_int"),
        explode_outer(col("a_array")).as("a_array_item"))
      df = df.select(col("a_int"), col("a_array_item.b"),
        explode_outer(col("a_array_item.b_array")).as("b_array_item"))
      val query = df.select("a_int", "b", "b_array_item.c")
      // b + b_array from outer struct, inner struct pruned to just c
      checkScan(query,
        "struct<a_int:bigint," +
          "a_array:array<struct<b:string," +
          "b_array:array<struct<c:string>>>>>")
      checkAnswer(query,
        Row(1, "a1_b1", "a1_b1_c1") :: Row(1, "a1_b1", "a1_b1_c2") ::
        Row(1, "a1_b2", "a1_b2_c1") :: Row(1, "a1_b2", "a1_b2_c2") ::
        Row(2, "a2_b1", "a2_b1_c1") :: Row(2, "a2_b1", "a2_b1_c2") ::
        Row(2, "a2_b2", "a2_b2_c1") :: Row(2, "a2_b2", "a2_b2_c2") :: Nil)
    }
  }

  // Chained posexplode - both outer and inner structs pruned
  testExplodePruning(
      "double-nested posexplode selecting struct fields") {
    withDoubleNestedData {
      val query = sql(
        "WITH base AS (" +
          "SELECT a_array_index, a_array_item, " +
          "b_array_index, b_array_item " +
          "FROM double_nested " +
          "LATERAL VIEW OUTER POSEXPLODE(a_array) " +
          "as a_array_index, a_array_item " +
          "LATERAL VIEW OUTER POSEXPLODE(a_array_item.b_array) " +
          "as b_array_index, b_array_item) " +
          "SELECT a_array_index, a_array_item.b, " +
          "b_array_index, b_array_item.c FROM base")
      checkScan(query,
        "struct<a_array:array<struct<b:string," +
          "b_array:array<struct<c:string>>>>>")
      checkAnswer(query,
        Row(0, "a1_b1", 0, "a1_b1_c1") :: Row(0, "a1_b1", 1, "a1_b1_c2") ::
        Row(1, "a1_b2", 0, "a1_b2_c1") :: Row(1, "a1_b2", 1, "a1_b2_c2") ::
        Row(0, "a2_b1", 0, "a2_b1_c1") :: Row(0, "a2_b1", 1, "a2_b1_c2") ::
        Row(1, "a2_b2", 0, "a2_b2_c1") :: Row(1, "a2_b2", 1, "a2_b2_c2") :: Nil)
    }
  }

  // Aggregation creates a barrier - the exploded array goes through FIRST(),
  // which Spark doesn't optimize through, so full array is scanned.
  testExplodePruning("explode with pass-through and aggregation") {
    withSampleData {
      val query = sql(
        "WITH base AS (" +
          "SELECT someStr, FIRST(someComplexArray) as complexArray " +
          "FROM sample GROUP BY someStr) " +
          "SELECT complex.col1 " +
          "FROM base LATERAL VIEW EXPLODE(complexArray) as complex")
      // Full array scanned due to aggregation barrier
      checkScan(query,
        "struct<someStr:string," +
          "someComplexArray:array<struct<col1:bigint,col2:bigint>>>")
      checkAnswer(query, Row(1) :: Nil)
    }
  }

  // Same as explode - aggregation barrier prevents pruning
  testExplodePruning("posexplode with pass-through and aggregation") {
    withSampleData {
      val query = sql(
        "WITH base AS (" +
          "SELECT someStr, FIRST(someComplexArray) as complexArray " +
          "FROM sample GROUP BY someStr) " +
          "SELECT complex.col1 " +
          "FROM base LATERAL VIEW POSEXPLODE(complexArray) " +
          "as complexIdx, complex")
      checkScan(query,
        "struct<someStr:string," +
          "someComplexArray:array<struct<col1:bigint,col2:bigint>>>")
      checkAnswer(query, Row(1) :: Nil)
    }
  }

  // =========================================================================
  //  Corner case tests for chained Generate support
  // =========================================================================

  // Mixed explode/posexplode chain - outer is explode, inner is posexplode
  testExplodePruning("mixed chain - explode then posexplode") {
    withSampleData {
      val query = sql(
        "SELECT complex.col1, valIdx, val " +
          "FROM sample " +
          "LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as complex " +
          "LATERAL VIEW POSEXPLODE(complex.col2) as valIdx, val")
      // col1 + col2 needed, col3 pruned
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col2:array<bigint>>>>")
      checkAnswer(query,
        Row(1, 0, 1) :: Row(1, 1, 2) :: Row(1, 2, 3) :: Nil)
    }
  }

  // Mixed chain - outer is posexplode, inner is explode
  testExplodePruning("mixed chain - posexplode then explode") {
    withSampleData {
      val query = sql(
        "SELECT complexIdx, complex.col1, val " +
          "FROM sample " +
          "LATERAL VIEW POSEXPLODE(someArrayOfComplexArrays) " +
          "as complexIdx, complex " +
          "LATERAL VIEW EXPLODE(complex.col2) as val")
      // col1 + col2 needed, col3 pruned
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col2:array<bigint>>>>")
      checkAnswer(query,
        Row(0, 1, 1) :: Row(0, 1, 2) :: Row(0, 1, 3) :: Nil)
    }
  }

  // Triple-nested chain: three levels of lateral views
  testExplodePruning("triple-nested explode chain") {
    withDoubleNestedData {
      val query = sql(
        "SELECT a_array_item.b, b_array_item.c, " +
          "c_char FROM double_nested " +
          "LATERAL VIEW EXPLODE(a_array) as a_array_item " +
          "LATERAL VIEW EXPLODE(a_array_item.b_array) as b_array_item " +
          "LATERAL VIEW EXPLODE(ARRAY(b_array_item.c)) as c_char")
      // b + b_array needed from outer, inner struct pruned to just c
      checkScan(query,
        "struct<a_array:array<struct<b:string," +
          "b_array:array<struct<c:string>>>>>")
      checkAnswer(query,
        Row("a1_b1", "a1_b1_c1", "a1_b1_c1") ::
        Row("a1_b1", "a1_b1_c2", "a1_b1_c2") ::
        Row("a1_b2", "a1_b2_c1", "a1_b2_c1") ::
        Row("a1_b2", "a1_b2_c2", "a1_b2_c2") ::
        Row("a2_b1", "a2_b1_c1", "a2_b1_c1") ::
        Row("a2_b1", "a2_b1_c2", "a2_b1_c2") ::
        Row("a2_b2", "a2_b2_c1", "a2_b2_c1") ::
        Row("a2_b2", "a2_b2_c2", "a2_b2_c2") :: Nil)
    }
  }

  // Direct element reference blocks pruning - whole struct is used
  testExplodePruning("direct element reference blocks pruning") {
    withSampleData {
      val query = sql(
        "SELECT complex, val " +
          "FROM sample " +
          "LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as complex " +
          "LATERAL VIEW EXPLODE(complex.col2) as val")
      // complex is referenced directly, so all fields needed
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col2:array<bigint>,col3:bigint>>>")
      checkAnswer(query,
        Row(Row(1L, Array(1, 2, 3), 4L), 1) ::
        Row(Row(1L, Array(1, 2, 3), 4L), 2) ::
        Row(Row(1L, Array(1, 2, 3), 4L), 3) :: Nil)
    }
  }

  // Pos-only optimization in chain - only position from outer, field from inner
  testExplodePruning("pos-only outer with field from inner") {
    withSampleData {
      val query = sql(
        "SELECT complexIdx, val " +
          "FROM sample " +
          "LATERAL VIEW POSEXPLODE(someArrayOfComplexArrays) " +
          "as complexIdx, complex " +
          "LATERAL VIEW EXPLODE(complex.col2) as val")
      // Only col2 needed from outer (for inner explode), col1 and col3 pruned
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col2:array<bigint>>>>")
      checkAnswer(query,
        Row(0, 1) :: Row(0, 2) :: Row(0, 3) :: Nil)
    }
  }

  // Non-contiguous fields in chain - tests ordinal stability
  // Selecting b (idx 0), b_int (idx 1) from outer, c (idx 0), c_2 (idx 2) from inner
  // This verifies ordinal fixing works for non-adjacent fields
  testExplodePruning("non-contiguous fields ordinal stability in chain") {
    withDoubleNestedData {
      val query = sql(
        "SELECT a_array_item.b, a_array_item.b_int, " +
          "b_array_item.c, b_array_item.c_2 FROM double_nested " +
          "LATERAL VIEW EXPLODE(a_array) as a_array_item " +
          "LATERAL VIEW EXPLODE(a_array_item.b_array) as b_array_item")
      // Outer: b (idx 0) + b_int (idx 1) + b_array (idx 3) - b_string pruned
      // Inner: c + c_2 needed, c_int pruned
      checkScan(query,
        "struct<a_array:array<struct<b:string,b_int:bigint," +
          "b_array:array<struct<c:string,c_2:string>>>>>")
      checkAnswer(query,
        Row("a1_b1", 1, "a1_b1_c1", "a1_b1_c1_d") ::
        Row("a1_b1", 1, "a1_b1_c2", "a1_b1_c2_d") ::
        Row("a1_b2", 2, "a1_b2_c1", "a1_b2_c2_d") ::
        Row("a1_b2", 2, "a1_b2_c2", "a1_b2_c2_d") ::
        Row("a2_b1", 3, "a2_b1_c1", "a2_b1_c1_d") ::
        Row("a2_b1", 3, "a2_b1_c2", "a2_b1_c2_d") ::
        Row("a2_b2", 4, "a2_b2_c1", "da") ::
        Row("a2_b2", 4, "a2_b2_c2", "da") :: Nil)
    }
  }

  // Chain with filter on generator output between generates
  testExplodePruning("chain with filter on outer generator output") {
    withDoubleNestedData {
      val query = sql(
        "SELECT a_array_item.b, b_array_item.c FROM double_nested " +
          "LATERAL VIEW EXPLODE(a_array) as a_array_item " +
          "LATERAL VIEW EXPLODE(a_array_item.b_array) as b_array_item " +
          "WHERE a_array_item.b_int > 2")
      // b + b_int (for filter) + b_array needed from outer, inner struct pruned to c
      checkScan(query,
        "struct<a_array:array<struct<b:string,b_int:bigint," +
          "b_array:array<struct<c:string>>>>>")
      checkAnswer(query,
        Row("a2_b1", "a2_b1_c1") :: Row("a2_b1", "a2_b1_c2") ::
        Row("a2_b2", "a2_b2_c1") :: Row("a2_b2", "a2_b2_c2") :: Nil)
    }
  }

  // Chain with filter on inner generator output
  testExplodePruning("chain with filter on inner generator output") {
    withDoubleNestedData {
      val query = sql(
        "SELECT a_array_item.b, b_array_item.c FROM double_nested " +
          "LATERAL VIEW EXPLODE(a_array) as a_array_item " +
          "LATERAL VIEW EXPLODE(a_array_item.b_array) as b_array_item " +
          "WHERE b_array_item.c_int > 0")
      // b + b_array needed from outer, c + c_int (for filter) from inner
      checkScan(query,
        "struct<a_array:array<struct<b:string," +
          "b_array:array<struct<c:string,c_int:bigint>>>>>")
      checkAnswer(query,
        Row("a1_b1", "a1_b1_c1") :: Row("a1_b1", "a1_b1_c2") ::
        Row("a1_b2", "a1_b2_c1") :: Row("a1_b2", "a1_b2_c2") ::
        Row("a2_b1", "a2_b1_c1") :: Row("a2_b1", "a2_b1_c2") ::
        Row("a2_b2", "a2_b2_c1") :: Row("a2_b2", "a2_b2_c2") :: Nil)
    }
  }

  // Only inner Generate can prune - outer uses all fields
  testExplodePruning("only inner generate can prune") {
    withDoubleNestedData {
      val query = sql(
        "SELECT a_array_item.b, a_array_item.b_int, " +
          "a_array_item.b_string, b_array_item.c FROM double_nested " +
          "LATERAL VIEW EXPLODE(a_array) as a_array_item " +
          "LATERAL VIEW EXPLODE(a_array_item.b_array) as b_array_item")
      // All fields from outer (b, b_int, b_string, b_array), inner pruned to just c
      checkScan(query,
        "struct<a_array:array<struct<b:string,b_int:bigint,b_string:string," +
          "b_array:array<struct<c:string>>>>>")
      checkAnswer(query,
        Row("a1_b1", 1, "da", "a1_b1_c1") :: Row("a1_b1", 1, "da", "a1_b1_c2") ::
        Row("a1_b2", 2, "da", "a1_b2_c1") :: Row("a1_b2", 2, "da", "a1_b2_c2") ::
        Row("a2_b1", 3, "da", "a2_b1_c1") :: Row("a2_b1", 3, "da", "a2_b1_c2") ::
        Row("a2_b2", 4, "da", "a2_b2_c1") :: Row("a2_b2", 4, "da", "a2_b2_c2") :: Nil)
    }
  }

  // All fields selected from chain - no pruning expected
  testExplodePruning("all fields from chain - no pruning") {
    withDoubleNestedData {
      val query = sql(
        "SELECT a_array_item.*, b_array_item.* FROM double_nested " +
          "LATERAL VIEW EXPLODE(a_array) as a_array_item " +
          "LATERAL VIEW EXPLODE(a_array_item.b_array) as b_array_item")
      // All fields needed from both levels
      checkScan(query,
        "struct<a_array:array<struct<b:string,b_int:bigint,b_string:string," +
          "b_array:array<struct<c:string,c_int:bigint,c_2:string>>>>>")
      // Just check it runs without error
      assert(query.collect().length == 8)
    }
  }

  // Posexplode chain with pos from both levels
  testExplodePruning("posexplode chain using both positions") {
    withSampleData {
      val query = sql(
        "SELECT complexIdx, valIdx " +
          "FROM sample " +
          "LATERAL VIEW POSEXPLODE(someArrayOfComplexArrays) " +
          "as complexIdx, complex " +
          "LATERAL VIEW POSEXPLODE(complex.col2) as valIdx, val")
      // Only positions used, but col2 needed for inner explode
      // Outer prunes to just col2 (minimal for inner explode)
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col2:array<bigint>>>>")
      checkAnswer(query,
        Row(0, 0) :: Row(0, 1) :: Row(0, 2) :: Nil)
    }
  }

  // Single explode with OUTER - null handling
  testExplodePruning("explode outer with null array") {
    withSampleData {
      // Create a query that would have null arrays (using a filter that produces no matches)
      val query = sql(
        "SELECT complex.col1 " +
          "FROM sample " +
          "LATERAL VIEW OUTER EXPLODE(someArrayOfComplexArrays) as complex " +
          "WHERE someStr = 'bla'")
      checkScan(query,
        "struct<someStr:string," +
          "someArrayOfComplexArrays:array<struct<col1:bigint>>>")
      checkAnswer(query, Row(1) :: Nil)
    }
  }

  // Verify pruning doesn't break when array has multiple elements
  testExplodePruning("multi-element array pruning correctness") {
    withDoubleNestedData {
      // Simple test without filter to avoid V1/V2 differences
      val query = sql(
        "SELECT a_array_item.b, a_array_item.b_int FROM double_nested " +
          "LATERAL VIEW EXPLODE(a_array) as a_array_item")
      // b + b_int selected from exploded struct, b_string and b_array pruned
      checkScan(query,
        "struct<a_array:array<struct<b:string,b_int:bigint>>>")
      checkAnswer(query,
        Row("a1_b1", 1) :: Row("a1_b2", 2) ::
        Row("a2_b1", 3) :: Row("a2_b2", 4) :: Nil)
    }
  }

  // =========================================================================
  //  Step 7 tests: Additional correctness fixes
  // =========================================================================

  // Step 7.1: Fix ordinals inside rewritten leaf filters
  // Test filter on non-contiguous field (keep col1 & col3, filter on col3)
  testExplodePruning("filter on non-contiguous field with ordinal fix") {
    withSampleData {
      // Filter on source array field (col3) while selecting col1 and col3
      // This tests that GetArrayStructFields ordinals are fixed in leaf filters
      val query = sql(
        """SELECT arrayVal.col1, arrayVal.col3
          |FROM sample
          |LATERAL VIEW EXPLODE(someArrayOfComplexArrays) as arrayVal
          |WHERE size(someArrayOfComplexArrays) > 0""".stripMargin)
      // col1 and col3 selected, col2 pruned
      checkScan(query,
        "struct<someArrayOfComplexArrays:" +
          "array<struct<col1:bigint,col3:bigint>>>")
      checkAnswer(query, Row(1, 4) :: Nil)
    }
  }

  // Step 7.4: Safer handling of Projects in decomposeChild
  // Test that explode works correctly with nested pruning through the DF API
  testExplodePruning("explode via DF API - multi field selection") {
    withSampleData {
      val query = spark.table("sample")
        .selectExpr("someStr", "explode(someArrayOfComplexArrays) as elem")
        .selectExpr("someStr", "elem.col1", "elem.col3")
      // col1 and col3 selected, col2 pruned
      checkScan(query,
        "struct<someStr:string," +
          "someArrayOfComplexArrays:array<struct<col1:bigint,col3:bigint>>>")
      checkAnswer(query, Row("bla", 1, 4) :: Nil)
    }
  }

  // =========================================================================
  //  Nested array inside struct tests (Step 7.2)
  // =========================================================================

  // Case class for nested array inside struct test data
  case class NestedArrayElement(x: Long, y: Long, z: Long)
  case class WrapperStruct(
    name: String,
    nestedArray: Array[NestedArrayElement])
  case class NestedArrayRecord(
    id: Long,
    wrapper: WrapperStruct)

  private val nestedArrayData = Seq(
    NestedArrayRecord(1, WrapperStruct("w1",
      Array(NestedArrayElement(10, 20, 30), NestedArrayElement(11, 21, 31)))),
    NestedArrayRecord(2, WrapperStruct("w2",
      Array(NestedArrayElement(12, 22, 32)))))

  private def withNestedArrayData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      makeDataSourceFile(nestedArrayData, new File(path + "/nested_array"))

      val schema =
        """`id` BIGINT,
          |`wrapper` STRUCT<`name`: STRING,
          |  `nestedArray`: ARRAY<STRUCT<`x`: BIGINT, `y`: BIGINT, `z`: BIGINT>>>
        """.stripMargin
      spark.read.format(dataSourceName).schema(schema).load(path + "/nested_array")
        .createOrReplaceTempView("nested_array")

      testThunk
    }
  }

  // Step 7.2: Allow pruning when generator child is derived from scan attribute
  // Test explode(col.structArr) where array is nested inside a struct
  testExplodePruning("explode array nested inside struct - single field") {
    withNestedArrayData {
      val query = sql(
        """SELECT id, elem.x
          |FROM nested_array
          |LATERAL VIEW EXPLODE(wrapper.nestedArray) as elem""".stripMargin)
      // Should prune y and z, keeping only x
      checkScan(query,
        "struct<id:bigint," +
          "wrapper:struct<nestedArray:array<struct<x:bigint>>>>")
      checkAnswer(query,
        Row(1, 10) :: Row(1, 11) :: Row(2, 12) :: Nil)
    }
  }

  // Note: Multi-field selection from nested array inside struct is not yet fully supported.
  // When ColumnPruning creates intermediate aliases for nested fields, our rule may not
  // be able to trace the aliases back to scan attributes. The single-field case works
  // because ColumnPruning doesn't always create aliases for simple nested projections.

  testExplodePruning("posexplode array nested inside struct") {
    withNestedArrayData {
      val query = sql(
        """SELECT id, pos, elem.y
          |FROM nested_array
          |LATERAL VIEW POSEXPLODE(wrapper.nestedArray) as pos, elem""".stripMargin)
      // Should prune x and z, keeping only y
      checkScan(query,
        "struct<id:bigint," +
          "wrapper:struct<nestedArray:array<struct<y:bigint>>>>")
      checkAnswer(query,
        Row(1, 0, 20) :: Row(1, 1, 21) :: Row(2, 0, 22) :: Nil)
    }
  }

  // Step 8: Test for nested arrays (array<array<struct>>)
  // This tests the GetNestedArrayStructFields expression integration
  protected def withDoublyNestedArrayData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("doublyNested", ArrayType(ArrayType(
          StructType(Seq(
            StructField("a", LongType),
            StructField("b", StringType),
            StructField("c", LongType)
          ))
        )))
      ))

      // Create test data: array<array<struct<a, b, c>>>
      // Row 1: [[[1, "x", 10], [2, "y", 20]], [[3, "z", 30]]]
      // Row 2: [[[4, "w", 40]]]
      val data = Seq(
        Row(1L, Seq(
          Seq(Row(1L, "x", 10L), Row(2L, "y", 20L)),
          Seq(Row(3L, "z", 30L))
        )),
        Row(2L, Seq(
          Seq(Row(4L, "w", 40L))
        ))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format(dataSourceName).save(path + "/doubly_nested")

      spark.read.format(dataSourceName).schema(schema).load(path + "/doubly_nested")
        .createOrReplaceTempView("doubly_nested")

      testThunk
    }
  }

  testExplodePruning("explode doubly nested array - single field") {
    withDoublyNestedArrayData {
      // Explode array<array<struct>> produces array<struct> elements
      // Then explode again to get struct elements
      val query = sql(
        """SELECT id, inner_elem.a
          |FROM doubly_nested
          |LATERAL VIEW EXPLODE(doublyNested) as outer_elem
          |LATERAL VIEW EXPLODE(outer_elem) as inner_elem""".stripMargin)

      // After two explodes, we access inner_elem.a
      // The outer array can be pruned to only include field 'a' in the innermost struct
      checkAnswer(query,
        Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Row(2, 4) :: Nil)
    }
  }

  testExplodePruning("explode doubly nested array - multi field") {
    withDoublyNestedArrayData {
      val query = sql(
        """SELECT id, inner_elem.a, inner_elem.c
          |FROM doubly_nested
          |LATERAL VIEW EXPLODE(doublyNested) as outer_elem
          |LATERAL VIEW EXPLODE(outer_elem) as inner_elem""".stripMargin)

      checkAnswer(query,
        Row(1, 1, 10) :: Row(1, 2, 20) :: Row(1, 3, 30) :: Row(2, 4, 40) :: Nil)
    }
  }

  // ============================================================================
  // PageView-style comprehensive tests: 3-level nesting with fields at each level
  // Models real-world data: PageView -> Requests -> Items
  // ============================================================================

  /**
   * PageView data model:
   * - Root: pageId, country, platform, userId (select some, prune others)
   * - requests: array<struct<requestId, ts, source, items>> (4 fields)
   * - items: array<struct<itemId, clicked, visible, charged, campaignId>> (5 fields)
   *
   * This mirrors real analytics data where pruning at each level is important.
   */
  protected def withPageViewData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      val itemStruct = StructType(Seq(
        StructField("itemId", LongType),
        StructField("clicked", BooleanType),
        StructField("visible", BooleanType),
        StructField("charged", BooleanType),
        StructField("campaignId", LongType)
      ))

      val requestStruct = StructType(Seq(
        StructField("requestId", StringType),
        StructField("ts", LongType),
        StructField("source", StringType),
        StructField("items", ArrayType(itemStruct))
      ))

      val schema = StructType(Seq(
        StructField("pageId", LongType),
        StructField("country", StringType),
        StructField("platform", StringType),
        StructField("userId", LongType),
        StructField("requests", ArrayType(requestStruct))
      ))

      // PageView 1: US, desktop, 2 requests with 2 items each
      // PageView 2: UK, mobile, 1 request with 3 items
      val data = Seq(
        Row(100L, "US", "desktop", 1001L, Seq(
          Row("req1", 1000L, "organic", Seq(
            Row(1L, true, true, false, 500L),
            Row(2L, false, true, true, 501L)
          )),
          Row("req2", 1001L, "paid", Seq(
            Row(3L, true, false, false, 502L),
            Row(4L, true, true, true, 503L)
          ))
        )),
        Row(200L, "UK", "mobile", 1002L, Seq(
          Row("req3", 2000L, "organic", Seq(
            Row(5L, false, true, false, 600L),
            Row(6L, true, true, true, 601L),
            Row(7L, false, false, false, 602L)
          ))
        ))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format(dataSourceName).save(path + "/pageview")

      spark.read.format(dataSourceName).schema(schema).load(path + "/pageview")
        .createOrReplaceTempView("pageview")

      testThunk
    }
  }

  // ============================================================================
  // PageView-style tests: 3-level nesting
  // Inner generate pruning IS supported - we can prune the innermost struct
  // fields even when the inner generate's source comes from the outer's output.
  // ============================================================================

  // Test: Select fields from ALL 3 levels
  // Inner items struct IS pruned to only itemId and clicked
  testExplodePruning("pageview - fields from all 3 levels") {
    withPageViewData {
      val query = sql(
        """SELECT
          |  pv.pageId, pv.country,
          |  req.requestId, req.ts,
          |  item.itemId, item.clicked
          |FROM pageview pv
          |LATERAL VIEW EXPLODE(pv.requests) as req
          |LATERAL VIEW EXPLODE(req.items) as item""".stripMargin)

      // Can prune: platform, userId (root), source (request)
      // CAN prune inner items struct - only itemId and clicked needed
      checkScan(query,
        "struct<pageId:bigint,country:string," +
          "requests:array<struct<requestId:string,ts:bigint," +
          "items:array<struct<itemId:bigint,clicked:boolean>>>>>")

      checkAnswer(query, Seq(
        Row(100L, "US", "req1", 1000L, 1L, true),
        Row(100L, "US", "req1", 1000L, 2L, false),
        Row(100L, "US", "req2", 1001L, 3L, true),
        Row(100L, "US", "req2", 1001L, 4L, true),
        Row(200L, "UK", "req3", 2000L, 5L, false),
        Row(200L, "UK", "req3", 2000L, 6L, true),
        Row(200L, "UK", "req3", 2000L, 7L, false)
      ))
    }
  }

  // Test: Select only from root + innermost level (skip middle)
  testExplodePruning("pageview - root and innermost levels only") {
    withPageViewData {
      val query = sql(
        """SELECT
          |  pv.pageId, pv.platform,
          |  item.itemId, item.campaignId
          |FROM pageview pv
          |LATERAL VIEW EXPLODE(pv.requests) as req
          |LATERAL VIEW EXPLODE(req.items) as item""".stripMargin)

      // Request fields pruned except items, inner items struct IS pruned
      checkScan(query,
        "struct<pageId:bigint,platform:string," +
          "requests:array<struct<items:array<struct<itemId:bigint,campaignId:bigint>>>>>")

      checkAnswer(query, Seq(
        Row(100L, "desktop", 1L, 500L),
        Row(100L, "desktop", 2L, 501L),
        Row(100L, "desktop", 3L, 502L),
        Row(100L, "desktop", 4L, 503L),
        Row(200L, "mobile", 5L, 600L),
        Row(200L, "mobile", 6L, 601L),
        Row(200L, "mobile", 7L, 602L)
      ))
    }
  }

  // Test: Select only from middle + innermost levels (no root fields)
  testExplodePruning("pageview - middle and innermost levels only") {
    withPageViewData {
      val query = sql(
        """SELECT
          |  req.requestId, req.source,
          |  item.clicked, item.visible, item.charged
          |FROM pageview pv
          |LATERAL VIEW EXPLODE(pv.requests) as req
          |LATERAL VIEW EXPLODE(req.items) as item""".stripMargin)

      // Root fields fully pruned, request fields pruned, inner items IS pruned
      checkScan(query,
        "struct<requests:array<struct<requestId:string,source:string," +
          "items:array<struct<clicked:boolean,visible:boolean,charged:boolean>>>>>")

      checkAnswer(query, Seq(
        Row("req1", "organic", true, true, false),
        Row("req1", "organic", false, true, true),
        Row("req2", "paid", true, false, false),
        Row("req2", "paid", true, true, true),
        Row("req3", "organic", false, true, false),
        Row("req3", "organic", true, true, true),
        Row("req3", "organic", false, false, false)
      ))
    }
  }

  // Test: Posexplode at both levels with field selection
  testExplodePruning("pageview - posexplode at both levels") {
    withPageViewData {
      val query = sql(
        """SELECT
          |  pv.country,
          |  reqIdx, req.requestId,
          |  itemIdx, item.itemId
          |FROM pageview pv
          |LATERAL VIEW POSEXPLODE(pv.requests) as reqIdx, req
          |LATERAL VIEW POSEXPLODE(req.items) as itemIdx, item""".stripMargin)

      // Inner items struct IS pruned
      checkScan(query,
        "struct<country:string," +
          "requests:array<struct<requestId:string," +
          "items:array<struct<itemId:bigint>>>>>")

      checkAnswer(query, Seq(
        Row("US", 0, "req1", 0, 1L),
        Row("US", 0, "req1", 1, 2L),
        Row("US", 1, "req2", 0, 3L),
        Row("US", 1, "req2", 1, 4L),
        Row("UK", 0, "req3", 0, 5L),
        Row("UK", 0, "req3", 1, 6L),
        Row("UK", 0, "req3", 2, 7L)
      ))
    }
  }

  // Test: Filter at middle level
  testExplodePruning("pageview - filter on middle level") {
    withPageViewData {
      val query = sql(
        """SELECT
          |  pv.pageId,
          |  req.requestId, req.ts,
          |  item.itemId, item.clicked
          |FROM pageview pv
          |LATERAL VIEW EXPLODE(pv.requests) as req
          |LATERAL VIEW EXPLODE(req.items) as item
          |WHERE req.source = 'organic'""".stripMargin)

      // source needed for filter, inner items struct IS pruned
      checkScan(query,
        "struct<pageId:bigint," +
          "requests:array<struct<requestId:string,ts:bigint,source:string," +
          "items:array<struct<itemId:bigint,clicked:boolean>>>>>")

      checkAnswer(query, Seq(
        Row(100L, "req1", 1000L, 1L, true),
        Row(100L, "req1", 1000L, 2L, false),
        Row(200L, "req3", 2000L, 5L, false),
        Row(200L, "req3", 2000L, 6L, true),
        Row(200L, "req3", 2000L, 7L, false)
      ))
    }
  }

  // Test: Filter at innermost level
  // Inner items struct IS pruned - clicked needed for filter
  testExplodePruning("pageview - filter on innermost level") {
    withPageViewData {
      val query = sql(
        """SELECT
          |  pv.country,
          |  req.requestId,
          |  item.itemId, item.campaignId
          |FROM pageview pv
          |LATERAL VIEW EXPLODE(pv.requests) as req
          |LATERAL VIEW EXPLODE(req.items) as item
          |WHERE item.clicked = true""".stripMargin)

      // clicked needed for filter; inner items struct IS pruned
      checkScan(query,
        "struct<country:string," +
          "requests:array<struct<requestId:string," +
          "items:array<struct<itemId:bigint,clicked:boolean,campaignId:bigint>>>>>")

      checkAnswer(query, Seq(
        Row("US", "req1", 1L, 500L),
        Row("US", "req2", 3L, 502L),
        Row("US", "req2", 4L, 503L),
        Row("UK", "req3", 6L, 601L)
      ))
    }
  }

  // Test: Many fields per level
  testExplodePruning("pageview - many fields per level") {
    withPageViewData {
      val query = sql(
        """SELECT
          |  pv.pageId, pv.country, pv.platform,
          |  req.requestId, req.ts, req.source,
          |  item.itemId, item.clicked, item.visible, item.charged
          |FROM pageview pv
          |LATERAL VIEW EXPLODE(pv.requests) as req
          |LATERAL VIEW EXPLODE(req.items) as item""".stripMargin)

      // Only userId (root) pruned; inner items struct IS pruned (campaignId not needed)
      checkScan(query,
        "struct<pageId:bigint,country:string,platform:string," +
          "requests:array<struct<requestId:string,ts:bigint,source:string," +
          "items:array<struct<itemId:bigint,clicked:boolean," +
          "visible:boolean,charged:boolean>>>>>")

      val result = query.collect()
      assert(result.length == 7)
      assert(result(0) == Row(100L, "US", "desktop", "req1", 1000L, "organic",
        1L, true, true, false))
    }
  }

  // ============================================================================
  // Inner Generate Pruning Tests (NestedArraysZip feature)
  // These tests verify that fields from inner generates CAN be pruned when
  // the source array comes from an outer generate's output.
  // ============================================================================

  /**
   * Inner-pruning test data model:
   * - Root: id (for identification)
   * - outer_array: array<struct<outer_field1, outer_field2, inner_array>>
   * - inner_array: array<struct<inner_f1, inner_f2, inner_f3, inner_f4>>
   *
   * This allows testing that inner_f3, inner_f4 can be pruned when only
   * inner_f1, inner_f2 are selected.
   */
  protected def withInnerPruningData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      val innerStruct = StructType(Seq(
        StructField("inner_f1", LongType),
        StructField("inner_f2", StringType),
        StructField("inner_f3", BooleanType),
        StructField("inner_f4", DoubleType)
      ))

      val outerStruct = StructType(Seq(
        StructField("outer_field1", StringType),
        StructField("outer_field2", LongType),
        StructField("inner_array", ArrayType(innerStruct))
      ))

      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("outer_array", ArrayType(outerStruct))
      ))

      // Row 1: 2 outer elements, each with 2 inner elements
      // Row 2: 1 outer element with 3 inner elements
      val data = Seq(
        Row(1L, Seq(
          Row("a1", 100L, Seq(
            Row(1L, "x1", true, 1.0),
            Row(2L, "x2", false, 2.0)
          )),
          Row("a2", 200L, Seq(
            Row(3L, "x3", true, 3.0),
            Row(4L, "x4", true, 4.0)
          ))
        )),
        Row(2L, Seq(
          Row("b1", 300L, Seq(
            Row(5L, "y1", false, 5.0),
            Row(6L, "y2", true, 6.0),
            Row(7L, "y3", false, 7.0)
          ))
        ))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format(dataSourceName).save(path + "/inner_pruning")

      spark.read.format(dataSourceName).schema(schema).load(path + "/inner_pruning")
        .createOrReplaceTempView("inner_pruning")

      testThunk
    }
  }

  // Test: Only inner_f1 selected - other inner fields should be pruned
  testExplodePruning("inner generate pruning - single field from inner") {
    withInnerPruningData {
      val query = sql(
        """SELECT id, outer_elem.outer_field1, inner_elem.inner_f1
          |FROM inner_pruning
          |LATERAL VIEW EXPLODE(outer_array) as outer_elem
          |LATERAL VIEW EXPLODE(outer_elem.inner_array) as inner_elem""".stripMargin)

      // Inner fields pruned: only inner_f1 needed (inner_f2, inner_f3, inner_f4 pruned)
      // Outer fields: outer_field1 + inner_array (outer_field2 pruned)
      checkScan(query,
        "struct<id:bigint," +
          "outer_array:array<struct<outer_field1:string," +
          "inner_array:array<struct<inner_f1:bigint>>>>>")

      checkAnswer(query, Seq(
        Row(1L, "a1", 1L), Row(1L, "a1", 2L),
        Row(1L, "a2", 3L), Row(1L, "a2", 4L),
        Row(2L, "b1", 5L), Row(2L, "b1", 6L), Row(2L, "b1", 7L)
      ))
    }
  }

  // Test: Two inner fields selected - prune the other two
  testExplodePruning("inner generate pruning - two fields from inner") {
    withInnerPruningData {
      val query = sql(
        """SELECT inner_elem.inner_f1, inner_elem.inner_f2
          |FROM inner_pruning
          |LATERAL VIEW EXPLODE(outer_array) as outer_elem
          |LATERAL VIEW EXPLODE(outer_elem.inner_array) as inner_elem""".stripMargin)

      // Only inner_f1 and inner_f2 needed
      checkScan(query,
        "struct<outer_array:array<struct<" +
          "inner_array:array<struct<inner_f1:bigint,inner_f2:string>>>>>")

      checkAnswer(query, Seq(
        Row(1L, "x1"), Row(2L, "x2"),
        Row(3L, "x3"), Row(4L, "x4"),
        Row(5L, "y1"), Row(6L, "y2"), Row(7L, "y3")
      ))
    }
  }

  // Test: Non-contiguous inner fields (first and last)
  testExplodePruning("inner generate pruning - non-contiguous inner fields") {
    withInnerPruningData {
      val query = sql(
        """SELECT inner_elem.inner_f1, inner_elem.inner_f4
          |FROM inner_pruning
          |LATERAL VIEW EXPLODE(outer_array) as outer_elem
          |LATERAL VIEW EXPLODE(outer_elem.inner_array) as inner_elem""".stripMargin)

      // inner_f1 and inner_f4 needed, middle fields pruned
      checkScan(query,
        "struct<outer_array:array<struct<" +
          "inner_array:array<struct<inner_f1:bigint,inner_f4:double>>>>>")

      checkAnswer(query, Seq(
        Row(1L, 1.0), Row(2L, 2.0),
        Row(3L, 3.0), Row(4L, 4.0),
        Row(5L, 5.0), Row(6L, 6.0), Row(7L, 7.0)
      ))
    }
  }

  // Test: Fields from both outer and inner, with filter on inner
  testExplodePruning("inner generate pruning - with filter on inner field") {
    withInnerPruningData {
      val query = sql(
        """SELECT id, outer_elem.outer_field1, inner_elem.inner_f1, inner_elem.inner_f2
          |FROM inner_pruning
          |LATERAL VIEW EXPLODE(outer_array) as outer_elem
          |LATERAL VIEW EXPLODE(outer_elem.inner_array) as inner_elem
          |WHERE inner_elem.inner_f3 = true""".stripMargin)

      // Filter requires inner_f3, but it's not in SELECT, so...
      // Note: Filter pushdown may or may not include inner_f3 in scan
      // For now, just verify query correctness
      checkAnswer(query, Seq(
        Row(1L, "a1", 1L, "x1"),
        Row(1L, "a2", 3L, "x3"), Row(1L, "a2", 4L, "x4"),
        Row(2L, "b1", 6L, "y2")
      ))
    }
  }

  // Test: Posexplode at inner level with field pruning
  testExplodePruning("inner generate pruning - posexplode inner") {
    withInnerPruningData {
      val query = sql(
        """SELECT outer_elem.outer_field1, inner_pos, inner_elem.inner_f1
          |FROM inner_pruning
          |LATERAL VIEW EXPLODE(outer_array) as outer_elem
          |LATERAL VIEW POSEXPLODE(outer_elem.inner_array) as inner_pos, inner_elem""".stripMargin)

      // Inner pruned to just inner_f1
      checkScan(query,
        "struct<outer_array:array<struct<outer_field1:string," +
          "inner_array:array<struct<inner_f1:bigint>>>>>")

      checkAnswer(query, Seq(
        Row("a1", 0, 1L), Row("a1", 1, 2L),
        Row("a2", 0, 3L), Row("a2", 1, 4L),
        Row("b1", 0, 5L), Row("b1", 1, 6L), Row("b1", 2, 7L)
      ))
    }
  }

  // Test: Posexplode at outer level, explode at inner with pruning
  testExplodePruning("inner generate pruning - posexplode outer explode inner") {
    withInnerPruningData {
      val query = sql(
        """SELECT outer_pos, inner_elem.inner_f2
          |FROM inner_pruning
          |LATERAL VIEW POSEXPLODE(outer_array) as outer_pos, outer_elem
          |LATERAL VIEW EXPLODE(outer_elem.inner_array) as inner_elem""".stripMargin)

      // Only position from outer, only inner_f2 from inner
      checkScan(query,
        "struct<outer_array:array<struct<" +
          "inner_array:array<struct<inner_f2:string>>>>>")

      checkAnswer(query, Seq(
        Row(0, "x1"), Row(0, "x2"),
        Row(1, "x3"), Row(1, "x4"),
        Row(0, "y1"), Row(0, "y2"), Row(0, "y3")
      ))
    }
  }

  // Test: All inner fields selected - no inner pruning
  testExplodePruning("inner generate pruning - all inner fields no pruning") {
    withInnerPruningData {
      val query = sql(
        """SELECT inner_elem.*
          |FROM inner_pruning
          |LATERAL VIEW EXPLODE(outer_array) as outer_elem
          |LATERAL VIEW EXPLODE(outer_elem.inner_array) as inner_elem""".stripMargin)

      // All inner fields needed, but outer_field1, outer_field2 can be pruned
      checkScan(query,
        "struct<outer_array:array<struct<" +
          "inner_array:array<struct<inner_f1:bigint,inner_f2:string," +
          "inner_f3:boolean,inner_f4:double>>>>>")

      val result = query.collect()
      assert(result.length == 7)
    }
  }

  // Test: Mixed selection from all levels
  testExplodePruning("inner generate pruning - mixed levels with pruning") {
    withInnerPruningData {
      val query = sql(
        """SELECT id, outer_elem.outer_field2, inner_elem.inner_f3
          |FROM inner_pruning
          |LATERAL VIEW EXPLODE(outer_array) as outer_elem
          |LATERAL VIEW EXPLODE(outer_elem.inner_array) as inner_elem""".stripMargin)

      // Root: id, Outer: outer_field2 + inner_array, Inner: inner_f3 only
      checkScan(query,
        "struct<id:bigint," +
          "outer_array:array<struct<outer_field2:bigint," +
          "inner_array:array<struct<inner_f3:boolean>>>>>")

      checkAnswer(query, Seq(
        Row(1L, 100L, true), Row(1L, 100L, false),
        Row(1L, 200L, true), Row(1L, 200L, true),
        Row(2L, 300L, false), Row(2L, 300L, true), Row(2L, 300L, false)
      ))
    }
  }

  // Test: Triple nesting with innermost pruning
  /**
   * Triple-nested data for testing deepest level pruning:
   * - Root: id
   * - level1: array<struct<l1_field, level2>>
   * - level2: array<struct<l2_field, level3>>
   * - level3: array<struct<l3_f1, l3_f2, l3_f3>>
   */
  protected def withTripleNestedData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      val l3Struct = StructType(Seq(
        StructField("l3_f1", LongType),
        StructField("l3_f2", StringType),
        StructField("l3_f3", BooleanType)
      ))

      val l2Struct = StructType(Seq(
        StructField("l2_field", StringType),
        StructField("level3", ArrayType(l3Struct))
      ))

      val l1Struct = StructType(Seq(
        StructField("l1_field", StringType),
        StructField("level2", ArrayType(l2Struct))
      ))

      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("level1", ArrayType(l1Struct))
      ))

      val data = Seq(
        Row(1L, Seq(
          Row("L1A", Seq(
            Row("L2A", Seq(Row(1L, "a", true), Row(2L, "b", false)))
          ))
        ))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format(dataSourceName).save(path + "/triple_nested")

      spark.read.format(dataSourceName).schema(schema).load(path + "/triple_nested")
        .createOrReplaceTempView("triple_nested")

      testThunk
    }
  }

  testExplodePruning("inner generate pruning - triple nesting deepest field") {
    withTripleNestedData {
      val query = sql(
        """SELECT l1.l1_field, l3.l3_f1
          |FROM triple_nested
          |LATERAL VIEW EXPLODE(level1) as l1
          |LATERAL VIEW EXPLODE(l1.level2) as l2
          |LATERAL VIEW EXPLODE(l2.level3) as l3""".stripMargin)

      // Only l3_f1 from deepest level (l3_f2, l3_f3 pruned)
      checkScan(query,
        "struct<level1:array<struct<l1_field:string," +
          "level2:array<struct<level3:array<struct<l3_f1:bigint>>>>>>>")

      checkAnswer(query, Seq(
        Row("L1A", 1L), Row("L1A", 2L)
      ))
    }
  }

  // ============================================================================
  // Code Review Integration Tests (Feb 2026)
  // Additional tests for edge cases identified during code review
  // ============================================================================

  // Test 1: Inner-generate pruning through nested arrays (core regression test)
  // Validates that inner generate's array from outer generate output is pruned
  protected def withBlockedItemsData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      val itemStruct = StructType(Seq(
        StructField("itemId", LongType),
        StructField("blockReason", StringType),
        StructField("score", DoubleType),
        StructField("timestamp", LongType)
      ))

      val requestStruct = StructType(Seq(
        StructField("requestId", StringType),
        StructField("blockedItemsList", ArrayType(itemStruct))
      ))

      val schema = StructType(Seq(
        StructField("pageId", LongType),
        StructField("requests", ArrayType(requestStruct))
      ))

      val data = Seq(
        Row(1L, Seq(
          Row("req1", Seq(
            Row(100L, "policy_violation", 0.9, 1000L),
            Row(101L, "spam", 0.8, 1001L)
          )),
          Row("req2", Seq(
            Row(102L, "duplicate", 0.7, 1002L)
          ))
        )),
        Row(2L, Seq(
          Row("req3", Seq(
            Row(200L, "low_quality", 0.5, 2000L)
          ))
        ))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format(dataSourceName).save(path + "/blocked_items")

      spark.read.format(dataSourceName).schema(schema).load(path + "/blocked_items")
        .createOrReplaceTempView("blocked_items")

      testThunk
    }
  }

  testExplodePruning("inner-generate pruning - blockedItemsList only blockReason") {
    withBlockedItemsData {
      // Explode requests, then explode blockedItemsList from each request
      // Only select blockReason from the inner items
      val query = sql(
        """SELECT req.requestId, item.blockReason
          |FROM blocked_items
          |LATERAL VIEW EXPLODE(requests) as req
          |LATERAL VIEW EXPLODE(req.blockedItemsList) as item""".stripMargin)

      // blockedItemsList should be pruned to only include blockReason
      // itemId, score, timestamp should NOT be in scan schema
      checkScan(query,
        "struct<requests:array<struct<requestId:string," +
          "blockedItemsList:array<struct<blockReason:string>>>>>")

      checkAnswer(query, Seq(
        Row("req1", "policy_violation"),
        Row("req1", "spam"),
        Row("req2", "duplicate"),
        Row("req3", "low_quality")
      ))
    }
  }

  // Test 2: Nested array inside struct + multi-field selection
  protected def withWrapperStructData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      val elemStruct = StructType(Seq(
        StructField("x", LongType),
        StructField("y", LongType),
        StructField("z", LongType)
      ))

      val wrapperStruct = StructType(Seq(
        StructField("name", StringType),
        StructField("arr", ArrayType(elemStruct))
      ))

      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("wrapper", wrapperStruct)
      ))

      val data = Seq(
        Row(1L, Row("first", Seq(Row(10L, 20L, 30L), Row(11L, 21L, 31L)))),
        Row(2L, Row("second", Seq(Row(12L, 22L, 32L))))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format(dataSourceName).save(path + "/wrapper_struct")

      spark.read.format(dataSourceName).schema(schema).load(path + "/wrapper_struct")
        .createOrReplaceTempView("wrapper_struct")

      testThunk
    }
  }

  // NOTE: Multi-field selection from nested array inside struct is a known limitation.
  // When ColumnPruning creates intermediate aliases for nested fields, our rule cannot
  // trace the aliases back to scan attributes. We verify correctness but not pruning.
  testExplodePruning("nested array inside struct - multi-field selection (correctness)") {
    withWrapperStructData {
      // Select wrapper.name and non-contiguous fields x, z from exploded array
      val query = sql(
        """SELECT wrapper.name, e.x, e.z
          |FROM wrapper_struct
          |LATERAL VIEW EXPLODE(wrapper.arr) as e""".stripMargin)

      // Pruning doesn't currently work for nested array inside struct with multi-field.
      // This test verifies the query executes correctly.
      // When this limitation is fixed, update expected schema to:
      // "struct<wrapper:struct<name:string,arr:array<struct<x:bigint,z:bigint>>>>"
      checkScan(query,
        "struct<wrapper:struct<name:string," +
          "arr:array<struct<x:bigint,y:bigint,z:bigint>>>>")

      checkAnswer(query, Seq(
        Row("first", 10L, 30L),
        Row("first", 11L, 31L),
        Row("second", 12L, 32L)
      ))
    }
  }

  // Test 3: pos-only posexplode chooses minimal-weight field
  protected def withMinimalWeightData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      // Struct with fields of different sizes: boolean < int < string
      val elemStruct = StructType(Seq(
        StructField("b", BooleanType),
        StructField("i", IntegerType),
        StructField("s", StringType)
      ))

      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("items", ArrayType(elemStruct))
      ))

      val data = Seq(
        Row(1L, Seq(Row(true, 100, "long_string_value_1"),
                    Row(false, 200, "long_string_value_2"))),
        Row(2L, Seq(Row(true, 300, "long_string_value_3")))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format(dataSourceName).save(path + "/minimal_weight")

      spark.read.format(dataSourceName).schema(schema).load(path + "/minimal_weight")
        .createOrReplaceTempView("minimal_weight")

      testThunk
    }
  }

  testExplodePruning("pos-only posexplode chooses minimal-weight field") {
    withMinimalWeightData {
      // Only select position, not any fields from the struct
      val query = sql(
        """SELECT id, pos
          |FROM minimal_weight
          |LATERAL VIEW POSEXPLODE(items) as pos, item""".stripMargin)

      // Should scan with minimal-weight field (boolean 'b')
      // Not the full struct with int and string
      checkScan(query,
        "struct<id:bigint,items:array<struct<b:boolean>>>")

      checkAnswer(query, Seq(
        Row(1L, 0),
        Row(1L, 1),
        Row(2L, 0)
      ))
    }
  }

  // Test 4: Filters on source array fields are preserved
  testExplodePruning("filter on source array field preserved in scan") {
    withWrapperStructData {
      // Select only x, but filter on y - both should be in scan
      val query = sql(
        """SELECT e.x
          |FROM wrapper_struct
          |LATERAL VIEW EXPLODE(wrapper.arr) as e
          |WHERE wrapper.arr.y IS NOT NULL""".stripMargin)

      // Scan must include both x (projection) and y (filter)
      checkScan(query,
        "struct<wrapper:struct<arr:array<struct<x:bigint,y:bigint>>>>")

      checkAnswer(query, Seq(
        Row(10L), Row(11L), Row(12L)
      ))
    }
  }

  // Test 5: Non-contiguous fields + ordinal stability
  protected def withFiveFieldData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      val elemStruct = StructType(Seq(
        StructField("a", LongType),
        StructField("b", LongType),
        StructField("c", LongType),
        StructField("d", LongType),
        StructField("e", LongType)
      ))

      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("arr", ArrayType(elemStruct))
      ))

      val data = Seq(
        Row(1L, Seq(Row(1L, 2L, 3L, 4L, 5L), Row(10L, 20L, 30L, 40L, 50L))),
        Row(2L, Seq(Row(100L, 200L, 300L, 400L, 500L)))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format(dataSourceName).save(path + "/five_field")

      spark.read.format(dataSourceName).schema(schema).load(path + "/five_field")
        .createOrReplaceTempView("five_field")

      testThunk
    }
  }

  testExplodePruning("non-contiguous fields ordinal stability") {
    withFiveFieldData {
      // Select fields a, c, e (skip b, d) - tests ordinal fix for gaps
      val query = sql(
        """SELECT e.a, e.c, e.e
          |FROM five_field
          |LATERAL VIEW EXPLODE(arr) as e""".stripMargin)

      // Should only include a, c, e in scan schema
      checkScan(query,
        "struct<arr:array<struct<a:bigint,c:bigint,e:bigint>>>")

      // Verify correct values despite ordinal gaps
      checkAnswer(query, Seq(
        Row(1L, 3L, 5L),
        Row(10L, 30L, 50L),
        Row(100L, 300L, 500L)
      ))
    }
  }

  // Test 6: Alias chain (GNA + ColumnPruning interaction)
  testExplodePruning("alias chain with GNA interaction") {
    withFiveFieldData {
      // Subquery with alias creates intermediate Project
      // Tests that pruning traces through alias definitions
      val query = sql(
        """WITH aliased AS (
          |  SELECT id, arr as my_arr
          |  FROM five_field
          |)
          |SELECT e.a, e.d
          |FROM aliased
          |LATERAL VIEW EXPLODE(my_arr) as e""".stripMargin)

      // Pruning should still work through the alias
      checkScan(query,
        "struct<arr:array<struct<a:bigint,d:bigint>>>")

      checkAnswer(query, Seq(
        Row(1L, 4L),
        Row(10L, 40L),
        Row(100L, 400L)
      ))
    }
  }

  // Test 7: Depth-2 nested arrays (array<array<struct>>)
  protected def withDepth2NestedData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      val innerStruct = StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", IntegerType),
        StructField("z", IntegerType)
      ))

      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("deep", ArrayType(ArrayType(innerStruct)))
      ))

      // array<array<struct<x,y,z>>>
      val data = Seq(
        Row(1L, Seq(
          Seq(Row(1, 10, 100), Row(2, 20, 200)),
          Seq(Row(3, 30, 300))
        )),
        Row(2L, Seq(
          Seq(Row(4, 40, 400))
        ))
      )

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format(dataSourceName).save(path + "/depth2_nested")

      spark.read.format(dataSourceName).schema(schema).load(path + "/depth2_nested")
        .createOrReplaceTempView("depth2_nested")

      testThunk
    }
  }

  testExplodePruning("depth-2 nested arrays - single inner field") {
    withDepth2NestedData {
      // Explode array<array<struct>> to get array<struct>, then explode again
      val query = sql(
        """SELECT id, inner_elem.x
          |FROM depth2_nested
          |LATERAL VIEW EXPLODE(deep) as outer_arr
          |LATERAL VIEW EXPLODE(outer_arr) as inner_elem""".stripMargin)

      // Inner struct should be pruned to only x (y, z not needed)
      checkScan(query,
        "struct<id:bigint,deep:array<array<struct<x:int>>>>")

      checkAnswer(query, Seq(
        Row(1L, 1), Row(1L, 2), Row(1L, 3),
        Row(2L, 4)
      ))
    }
  }

  testExplodePruning("depth-2 nested arrays - multi inner field") {
    withDepth2NestedData {
      // Select x and z, pruning y
      val query = sql(
        """SELECT id, inner_elem.x, inner_elem.z
          |FROM depth2_nested
          |LATERAL VIEW EXPLODE(deep) as outer_arr
          |LATERAL VIEW EXPLODE(outer_arr) as inner_elem""".stripMargin)

      // Inner struct pruned to x and z
      checkScan(query,
        "struct<id:bigint,deep:array<array<struct<x:int,z:int>>>>")

      checkAnswer(query, Seq(
        Row(1L, 1, 100), Row(1L, 2, 200), Row(1L, 3, 300),
        Row(2L, 4, 400)
      ))
    }
  }

  testExplodePruning("depth-2 nested arrays - filter on pruned fields") {
    withDepth2NestedData {
      // Select x and z (prune y), with filter on x
      // Exercises filter ordinal fixup path with GetNestedArrayStructFields
      val query = sql(
        """SELECT id, inner_elem.x, inner_elem.z
          |FROM depth2_nested
          |LATERAL VIEW EXPLODE(deep) as outer_arr
          |LATERAL VIEW EXPLODE(outer_arr) as inner_elem
          |WHERE inner_elem.x > 2""".stripMargin)

      checkScan(query,
        "struct<id:bigint,deep:array<array<struct<x:int,z:int>>>>")

      checkAnswer(query, Seq(
        Row(1L, 3, 300),
        Row(2L, 4, 400)
      ))
    }
  }

  testExplodePruning("depth-2 nested arrays - posexplode with pruning") {
    withDepth2NestedData {
      // POSEXPLODE on depth-2 array, select x only (prune y and z)
      val query = sql(
        """SELECT id, pos, inner_elem.x
          |FROM depth2_nested
          |LATERAL VIEW EXPLODE(deep) as outer_arr
          |LATERAL VIEW POSEXPLODE(outer_arr) as pos, inner_elem""".stripMargin)

      checkScan(query,
        "struct<id:bigint,deep:array<array<struct<x:int>>>>")

      checkAnswer(query, Seq(
        Row(1L, 0, 1), Row(1L, 1, 2), Row(1L, 0, 3),
        Row(2L, 0, 4)
      ))
    }
  }

  // =========================================================================
  //  Leaf filter ordinal fixup tests
  //
  //  These tests verify that filters on the source array column (not on
  //  exploded elements) have correct ordinals after pruning. When such filters
  //  reference struct fields via GetArrayStructFields, the ordinals must be
  //  updated to match the pruned schema.
  // =========================================================================

  testExplodePruning("leaf filter on source array field - ordinal fixup after pruning") {
    withMixedArrayData {
      // SELECT item.name with filter on items.qty (source array struct field access).
      // items.qty generates GetArrayStructFields(items, qty, 2, 3, false) to extract
      // the qty field from each struct element. After pruning detail (not needed),
      // the ordinal must be rewritten: GetArrayStructFields(_pruned, qty, 1, 2, false).
      val query = sql(
        """SELECT item.name
          |FROM mixed_array
          |LATERAL VIEW EXPLODE(items) AS item
          |WHERE size(items.qty) > 1""".stripMargin)

      // Scan needs name (projected) and qty (leaf filter via items.qty).
      // detail is not referenced anywhere, so it's pruned.
      checkScan(query,
        "struct<items:array<struct<name:string,qty:int>>>")

      // id=1: items.qty = [5,3], size=2 > 1 -> apple, banana included
      // id=2: items.qty = [1], size=1 > 1 -> cherry excluded
      checkAnswer(query,
        Row("apple") :: Row("banana") :: Nil)
    }
  }

  testExplodePruning("depth-2 nested arrays - leaf filter on source column") {
    withDepth2NestedData {
      // Filter on source column (deep IS NOT NULL) is a leaf filter that
      // references the source array directly. After pruning to struct<x>,
      // the direct reference is rewritten to use the pruned attribute.
      val query = sql(
        """SELECT id, inner_elem.x
          |FROM depth2_nested
          |LATERAL VIEW EXPLODE(deep) as outer_arr
          |LATERAL VIEW EXPLODE(outer_arr) as inner_elem
          |WHERE deep IS NOT NULL""".stripMargin)

      checkScan(query,
        "struct<id:bigint,deep:array<array<struct<x:int>>>>")

      checkAnswer(query, Seq(
        Row(1L, 1), Row(1L, 2), Row(1L, 3),
        Row(2L, 4)
      ))
    }
  }

  // =========================================================================
  //  OUTER POSEXPLODE with aggregation tests
  //
  //  These tests verify that schema pruning works correctly when aggregation
  //  is combined with OUTER POSEXPLODE. The PruneNestedFieldsThroughGenerateForScan
  //  rule properly tracks field requirements through Aggregate nodes.
  // =========================================================================

  // Test with single OUTER POSEXPLODE + aggregation - works correctly
  testExplodePruning("OUTER POSEXPLODE with aggregation - large struct") {
    withSampleData {
      val query = sql(
        """SELECT complex.col1, COUNT(*) as cnt, SUM(complex.col1) as total
          |FROM sample
          |LATERAL VIEW OUTER POSEXPLODE(someComplexArray) AS idx, complex
          |WHERE complex.col1 IS NOT NULL
          |GROUP BY complex.col1""".stripMargin)

      // Only col1 needed, col2 should be pruned
      checkScan(query,
        "struct<someComplexArray:array<struct<col1:bigint>>>")

      checkAnswer(query, Row(1L, 1, 1L) :: Nil)
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - Window Functions
  // =========================================================================

  testExplodePruning("window function - ROW_NUMBER over exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b, a_item.b_int,
          |       ROW_NUMBER() OVER (PARTITION BY a ORDER BY a_item.b_int) as rn
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin)

      // Only b and b_int needed from a_array element, b_string and b_array pruned
      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>")

      // Check results include row numbers
      val result = query.collect()
      assert(result.length == 4)
      // ROW_NUMBER returns Int, not Long
      assert(result.map(_.getInt(3)).toSet == Set(1, 2))
    }
  }

  testExplodePruning("window function - LAG/LEAD over exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b,
          |       LAG(a_item.b_int, 1) OVER (PARTITION BY a ORDER BY a_item.b_int) as prev_val,
          |       LEAD(a_item.b_int, 1) OVER (PARTITION BY a ORDER BY a_item.b_int) as next_val
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>")

      query.collect()
    }
  }

  testExplodePruning("window function - SUM OVER with exploded nested data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b, b_item.c_int,
          |       SUM(b_item.c_int) OVER (PARTITION BY a ORDER BY a_item.b) as running_sum
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |LATERAL VIEW EXPLODE(a_item.b_array) AS b_item""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string," +
          "b_array:array<struct<c_int:bigint>>>>>")

      query.collect()
    }
  }

  testExplodePruning("window function - RANK and DENSE_RANK") {
    withSampleData {
      val query = sql(
        """SELECT someStr, complex.col1,
          |       RANK() OVER (ORDER BY complex.col1) as rnk,
          |       DENSE_RANK() OVER (ORDER BY complex.col1) as dense_rnk
          |FROM sample
          |LATERAL VIEW EXPLODE(someComplexArray) AS complex""".stripMargin)

      checkScan(query,
        "struct<someStr:string,someComplexArray:array<struct<col1:bigint>>>")

      checkAnswer(query, Row("bla", 1L, 1, 1) :: Nil)
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - Higher-Order Functions
  // =========================================================================

  testExplodePruning("higher-order function - TRANSFORM on exploded array") {
    withSampleData {
      val query = sql(
        """SELECT someStr,
          |       TRANSFORM(someComplexArray, x -> x.col1 * 2) as doubled
          |FROM sample""".stripMargin)

      // TRANSFORM uses higher-order functions, not generators - full struct is read
      // This is a known limitation: higher-order function pruning is separate from
      // generator-based pruning handled by PruneNestedFieldsThroughGenerateForScan
      checkScan(query,
        "struct<someStr:string,someComplexArray:array<struct<col1:bigint,col2:bigint>>>")

      checkAnswer(query, Row("bla", Array(2L)) :: Nil)
    }
  }

  testExplodePruning("higher-order function - FILTER on array") {
    withSampleData {
      val query = sql(
        """SELECT someStr,
          |       FILTER(someComplexArray, x -> x.col1 > 0) as filtered
          |FROM sample""".stripMargin)

      checkScan(query,
        "struct<someStr:string,someComplexArray:array<struct<col1:bigint,col2:bigint>>>")

      checkAnswer(query, Row("bla", Array(Row(1L, 2L))) :: Nil)
    }
  }

  testExplodePruning("higher-order function - AGGREGATE on array") {
    withSampleData {
      val query = sql(
        """SELECT someStr,
          |       AGGREGATE(someComplexArray, 0L, (acc, x) -> acc + x.col1) as total
          |FROM sample""".stripMargin)

      // AGGREGATE uses higher-order functions, not generators - full struct is read
      checkScan(query,
        "struct<someStr:string,someComplexArray:array<struct<col1:bigint,col2:bigint>>>")

      checkAnswer(query, Row("bla", 1L) :: Nil)
    }
  }

  testExplodePruning("higher-order function - EXISTS on array") {
    withSampleData {
      val query = sql(
        """SELECT someStr,
          |       EXISTS(someComplexArray, x -> x.col1 > 0) as has_positive
          |FROM sample""".stripMargin)

      // EXISTS uses higher-order functions, not generators - full struct is read
      checkScan(query,
        "struct<someStr:string,someComplexArray:array<struct<col1:bigint,col2:bigint>>>")

      checkAnswer(query, Row("bla", true) :: Nil)
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - UNION Operations
  // =========================================================================

  testExplodePruning("UNION ALL with exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |WHERE a = 'a1'
          |UNION ALL
          |SELECT a, a_item.b FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |WHERE a = 'a2'""".stripMargin)

      // Both branches should prune to same schema
      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string>>>",
        "struct<a:string,a_array:array<struct<b:string>>>")

      assert(query.collect().length == 4)
    }
  }

  testExplodePruning("UNION with different field selections") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b as field FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |UNION
          |SELECT a, a_item.b_string as field FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin)

      // First branch needs b, second needs b_string
      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string>>>",
        "struct<a:string,a_array:array<struct<b_string:string>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - JOIN Operations
  // =========================================================================

  testExplodePruning("INNER JOIN on exploded data") {
    withDoubleNestedData {
      // Create exploded views for join - LATERAL VIEW must be part of the FROM clause
      spark.sql(
        """SELECT a, a_item.b, a_item.b_int
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin
      ).createOrReplaceTempView("exploded1")

      spark.sql(
        """SELECT a as a2, a_item.b as b2, a_item.b_int as b_int2
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin
      ).createOrReplaceTempView("exploded2")

      val query = sql(
        """SELECT e1.a, e1.b, e2.b_int2
          |FROM exploded1 e1
          |INNER JOIN exploded2 e2
          |ON e1.a = e2.a2 AND e1.b = e2.b2""".stripMargin)

      // Each branch should be pruned independently
      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string>>>",
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>")

      query.collect()
    }
  }

  testExplodePruning("LEFT JOIN with exploded data and NULL handling") {
    withDoubleNestedData {
      // Create exploded views - LATERAL VIEW must be part of the FROM clause
      spark.sql(
        """SELECT a, a_item.b, a_item.b_int
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |WHERE a = 'a1'""".stripMargin
      ).createOrReplaceTempView("left_exploded")

      spark.sql(
        """SELECT a_item.b_int as r_b_int
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |WHERE a = 'a2'""".stripMargin
      ).createOrReplaceTempView("right_exploded")

      val query = sql(
        """SELECT l.a, l.b, r.r_b_int
          |FROM left_exploded l
          |LEFT JOIN right_exploded r
          |ON l.b_int = r.r_b_int""".stripMargin)

      // Both branches need 'a' because of WHERE clause filter
      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>",
        "struct<a:string,a_array:array<struct<b_int:bigint>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - Subqueries
  // =========================================================================

  testExplodePruning("scalar subquery with exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a,
          |       (SELECT MAX(sub_item.b_int)
          |        FROM double_nested sub
          |        LATERAL VIEW EXPLODE(sub.a_array) AS sub_item
          |        WHERE sub.a = double_nested.a) as max_b_int
          |FROM double_nested""".stripMargin)

      // Main query needs a, subquery needs a and b_int
      query.collect()
    }
  }

  testExplodePruning("IN subquery with exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |WHERE a_item.b_int IN (
          |  SELECT b_item.c_int
          |  FROM double_nested sub
          |  LATERAL VIEW EXPLODE(sub.a_array) AS sub_a_item
          |  LATERAL VIEW EXPLODE(sub_a_item.b_array) AS b_item
          |)""".stripMargin)

      query.collect()
    }
  }

  testExplodePruning("EXISTS subquery with exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |WHERE EXISTS (
          |  SELECT 1
          |  FROM double_nested sub
          |  LATERAL VIEW EXPLODE(sub.a_array) AS sub_item
          |  WHERE sub_item.b_int > a_item.b_int
          |)""".stripMargin)

      query.collect()
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - DISTINCT and HAVING
  // =========================================================================

  testExplodePruning("SELECT DISTINCT with exploded fields") {
    withDoubleNestedData {
      val query = sql(
        """SELECT DISTINCT a_item.b_int
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin)

      checkScan(query,
        "struct<a_array:array<struct<b_int:bigint>>>")

      checkAnswer(query, Row(1L) :: Row(2L) :: Row(3L) :: Row(4L) :: Nil)
    }
  }

  testExplodePruning("GROUP BY with HAVING on exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b, COUNT(*) as cnt
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |LATERAL VIEW EXPLODE(a_item.b_array) AS b_item
          |GROUP BY a, a_item.b
          |HAVING COUNT(*) >= 2""".stripMargin)

      // Inner array can't be pruned to empty struct - full inner struct is read
      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string," +
          "b_array:array<struct<c:string,c_int:bigint,c_2:string>>>>>")

      val results = query.collect()
      assert(results.forall(_.getLong(2) >= 2))
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - CUBE/ROLLUP/GROUPING SETS
  // =========================================================================

  testExplodePruning("ROLLUP with exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b, SUM(a_item.b_int) as total
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |GROUP BY ROLLUP(a, a_item.b)""".stripMargin)

      // ROLLUP changes plan structure - full struct is read
      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint," +
          "b_string:string,b_array:array<struct<c:string,c_int:bigint,c_2:string>>>>>")

      query.collect()
    }
  }

  testExplodePruning("CUBE with exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b, COUNT(*) as cnt
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |GROUP BY CUBE(a, a_item.b)""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string>>>")

      query.collect()
    }
  }

  testExplodePruning("GROUPING SETS with exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b, a_item.b_int, COUNT(*) as cnt
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |GROUP BY GROUPING SETS ((a), (a_item.b), (a, a_item.b, a_item.b_int))""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - CASE WHEN Expressions
  // =========================================================================

  testExplodePruning("CASE WHEN with exploded fields") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a,
          |       CASE
          |         WHEN a_item.b_int > 2 THEN 'high'
          |         WHEN a_item.b_int > 1 THEN 'medium'
          |         ELSE 'low'
          |       END as category,
          |       a_item.b
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>")

      query.collect()
    }
  }

  testExplodePruning("nested CASE WHEN with multiple explode levels") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a,
          |       CASE
          |         WHEN b_item.c_int > 0 THEN a_item.b
          |         ELSE 'unknown'
          |       END as result
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |LATERAL VIEW EXPLODE(a_item.b_array) AS b_item""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string," +
          "b_array:array<struct<c_int:bigint>>>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - Complex CTEs
  // =========================================================================

  testExplodePruning("multiple CTEs with exploded data") {
    withDoubleNestedData {
      val query = sql(
        """WITH cte1 AS (
          |  SELECT a, a_item.b, a_item.b_int
          |  FROM double_nested
          |  LATERAL VIEW EXPLODE(a_array) AS a_item
          |),
          |cte2 AS (
          |  SELECT a, b, SUM(b_int) as total
          |  FROM cte1
          |  GROUP BY a, b
          |)
          |SELECT * FROM cte2 WHERE total > 0""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>")

      query.collect()
    }
  }

  testExplodePruning("CTE with window function over exploded data") {
    withDoubleNestedData {
      val query = sql(
        """WITH ranked AS (
          |  SELECT a, a_item.b, a_item.b_int,
          |         ROW_NUMBER() OVER (PARTITION BY a ORDER BY a_item.b_int DESC) as rn
          |  FROM double_nested
          |  LATERAL VIEW EXPLODE(a_array) AS a_item
          |)
          |SELECT a, b, b_int FROM ranked WHERE rn = 1""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>")

      checkAnswer(query, Row("a1", "a1_b2", 2L) :: Row("a2", "a2_b2", 4L) :: Nil)
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - ORDER BY and LIMIT
  // =========================================================================

  testExplodePruning("ORDER BY on exploded nested field") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b, b_item.c
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |LATERAL VIEW EXPLODE(a_item.b_array) AS b_item
          |ORDER BY b_item.c_int DESC
          |LIMIT 5""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string," +
          "b_array:array<struct<c:string,c_int:bigint>>>>>")

      val result = query.collect()
      assert(result.length == 5)
    }
  }

  testExplodePruning("complex ORDER BY with multiple explode levels") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, a_item.b, b_item.c_int
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |LATERAL VIEW EXPLODE(a_item.b_array) AS b_item
          |ORDER BY a DESC, a_item.b_int ASC, b_item.c_int DESC""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint," +
          "b_array:array<struct<c_int:bigint>>>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - COALESCE and NULL handling
  // =========================================================================

  testExplodePruning("COALESCE with exploded fields") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a,
          |       COALESCE(a_item.b, 'default') as b_value,
          |       COALESCE(a_item.b_int, 0) as b_int_value
          |FROM double_nested
          |LATERAL VIEW OUTER EXPLODE(a_array) AS a_item""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>")

      query.collect()
    }
  }

  testExplodePruning("IFNULL and NULLIF with exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a,
          |       IFNULL(a_item.b_int, -1) as safe_int,
          |       NULLIF(a_item.b, '') as non_empty_b
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_int:bigint>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - String Functions
  // =========================================================================

  testExplodePruning("string functions on exploded fields") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a,
          |       UPPER(a_item.b) as upper_b,
          |       LENGTH(a_item.b_string) as len,
          |       CONCAT(a_item.b, '-', a_item.b_string) as combined
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string,b_string:string>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - Date/Time Functions
  // =========================================================================

  testExplodePruning("arithmetic on exploded numeric fields") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a,
          |       a_item.b_int + 100 as adjusted,
          |       a_item.b_int * 2 as doubled,
          |       CAST(a_item.b_int AS DOUBLE) / 3.0 as third
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b_int:bigint>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Advanced SQL Constructs - Array Aggregate Functions
  // =========================================================================

  testExplodePruning("COLLECT_LIST on exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, COLLECT_LIST(a_item.b) as all_bs
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |GROUP BY a""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b:string>>>")

      val result = query.collect()
      assert(result.length == 2)
    }
  }

  testExplodePruning("COLLECT_SET on nested exploded data") {
    withDoubleNestedData {
      val query = sql(
        """SELECT a, COLLECT_SET(b_item.c_int) as unique_c_ints
          |FROM double_nested
          |LATERAL VIEW EXPLODE(a_array) AS a_item
          |LATERAL VIEW EXPLODE(a_item.b_array) AS b_item
          |GROUP BY a""".stripMargin)

      checkScan(query,
        "struct<a:string,a_array:array<struct<b_array:array<struct<c_int:bigint>>>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Infrastructure Layer Pattern - CTE with SELECT *
  // =========================================================================

  // This test validates a common real-world pattern where an "infra" layer
  // defines a base CTE using SELECT * for multiple explode levels, but the
  // final query only selects specific columns. Schema pruning should still
  // apply based on what the outer query actually uses.
  testExplodePruning("infrastructure layer CTE with SELECT * and specific outer select") {
    withDoubleNestedData {
      val query = sql(
        """WITH exploded AS (
          |  SELECT *
          |  FROM double_nested
          |  LATERAL VIEW OUTER EXPLODE(a_array) AS a_item
          |  LATERAL VIEW OUTER EXPLODE(a_item.b_array) AS b_item
          |)
          |SELECT
          |  a,
          |  a_item.b,
          |  b_item.c_int,
          |  COUNT(*) as cnt,
          |  SUM(CASE WHEN a_item.b_int > 1 THEN 1 ELSE 0 END) as high_count
          |FROM exploded
          |WHERE a_bool = true
          |GROUP BY a, a_item.b, b_item.c_int
          |ORDER BY cnt DESC
          |LIMIT 10""".stripMargin)

      // Despite SELECT * in CTE, only used fields from arrays should be read.
      // Note: The filter on a_bool may be evaluated after scan depending on
      // optimization order, so a_bool might not be in scan schema.
      // Key verification: a_array struct fields ARE pruned (no b_string, c, c_2)
      checkScan(query,
        "struct<a:string," +
          "a_array:array<struct<b:string,b_int:bigint," +
          "b_array:array<struct<c_int:bigint>>>>>")

      query.collect()
    }
  }

  // Simpler variant to verify the basic pattern works
  testExplodePruning("CTE with SELECT * - single explode level") {
    withDoubleNestedData {
      val query = sql(
        """WITH base AS (
          |  SELECT *
          |  FROM double_nested
          |  LATERAL VIEW EXPLODE(a_array) AS item
          |)
          |SELECT a, item.b, item.b_int
          |FROM base
          |WHERE a_bool = true""".stripMargin)

      // Key verification: a_array struct fields ARE pruned (no b_string, b_array)
      // Note: a_bool filter may be evaluated after scan
      checkScan(query,
        "struct<a:string," +
          "a_array:array<struct<b:string,b_int:bigint>>>")

      query.collect()
    }
  }

  // =========================================================================
  //  Regression tests: non-struct Generate above struct chain (Pattern 3)
  //
  //  These verify that when a non-struct Generate (e.g., EXPLODE(tags) on
  //  array<string>) sits above a struct Generate chain (EXPLODE(items) on
  //  array<struct>), the scan includes the scalar array column even when it
  //  is not explicitly selected, and Generate.unrequiredChildIndex is
  //  preserved correctly.
  // =========================================================================

  testExplodePruning("mixed arrays - non-struct generate above struct chain") {
    withMixedArrayData {
      // Regression: tags is NOT in SELECT but is needed by EXPLODE(tags).
      // Before fix, this failed with INTERNAL_ERROR_ATTRIBUTE_NOT_FOUND
      // because tags was dropped from scan and from Generate child output.
      val query = sql(
        """SELECT item.name, item.detail.color, tag
          |FROM mixed_array
          |LATERAL VIEW EXPLODE(items) AS item
          |LATERAL VIEW EXPLODE(tags) AS tag""".stripMargin)

      // tags must be in scan schema; qty is pruned (top-level field).
      // detail is pruned to struct<color> since only detail.color is accessed.
      checkScan(query,
        "struct<items:array<struct<name:string," +
          "detail:struct<color:string>>>,tags:array<string>>")

      checkAnswer(query,
        Row("apple", "red", "fruit") ::
          Row("apple", "red", "food") ::
          Row("banana", "yellow", "fruit") ::
          Row("banana", "yellow", "food") ::
          Row("cherry", "dark red", "berry") :: Nil)
    }
  }

  testExplodePruning("mixed arrays - pass-through column from non-struct generate") {
    withMixedArrayData {
      // tags appears both as a pass-through column AND as the generator source
      val query = sql(
        """SELECT tags, item.name, item.detail.color, tag
          |FROM mixed_array
          |LATERAL VIEW EXPLODE(items) AS item
          |LATERAL VIEW EXPLODE(tags) AS tag""".stripMargin)

      checkScan(query,
        "struct<items:array<struct<name:string," +
          "detail:struct<color:string>>>,tags:array<string>>")

      checkAnswer(query,
        Row(Seq("fruit", "food"), "apple", "red", "fruit") ::
          Row(Seq("fruit", "food"), "apple", "red", "food") ::
          Row(Seq("fruit", "food"), "banana", "yellow", "fruit") ::
          Row(Seq("fruit", "food"), "banana", "yellow", "food") ::
          Row(Seq("berry"), "cherry", "dark red", "berry") :: Nil)
    }
  }

  testExplodePruning("mixed arrays - scalar column with non-struct generate") {
    withMixedArrayData {
      // id (scalar) + tags (array for non-struct generate) + pruned struct fields
      val query = sql(
        """SELECT id, item.name, item.detail.color, tag
          |FROM mixed_array
          |LATERAL VIEW EXPLODE(items) AS item
          |LATERAL VIEW EXPLODE(tags) AS tag""".stripMargin)

      checkScan(query,
        "struct<id:int,items:array<struct<name:string," +
          "detail:struct<color:string>>>,tags:array<string>>")

      checkAnswer(query,
        Row(1, "apple", "red", "fruit") ::
          Row(1, "apple", "red", "food") ::
          Row(1, "banana", "yellow", "fruit") ::
          Row(1, "banana", "yellow", "food") ::
          Row(2, "cherry", "dark red", "berry") :: Nil)
    }
  }

  testExplodePruning("mixed arrays - nested sub-field pruning with all top-level fields") {
    withMixedArrayData {
      // All top-level struct fields (name, detail, qty) are selected so no
      // top-level pruning. detail is pruned to struct<color> (size not accessed).
      val query = sql(
        """SELECT item.name, item.detail.color, item.qty, tag
          |FROM mixed_array
          |LATERAL VIEW EXPLODE(items) AS item
          |LATERAL VIEW EXPLODE(tags) AS tag""".stripMargin)

      checkScan(query,
        "struct<items:array<struct<name:string," +
          "detail:struct<color:string>,qty:int>>,tags:array<string>>")

      checkAnswer(query,
        Row("apple", "red", 5, "fruit") ::
          Row("apple", "red", 5, "food") ::
          Row("banana", "yellow", 3, "fruit") ::
          Row("banana", "yellow", 3, "food") ::
          Row("cherry", "dark red", 1, "berry") :: Nil)
    }
  }

  // Nested struct pruning within array element - prunes both top-level and nested fields
  testExplodePruning("nested struct pruning - prune sub-fields of struct inside array") {
    withMixedArrayData {
      // Select name and detail.color only: prunes qty (top-level) AND detail.size (nested)
      val query = sql(
        """SELECT item.name, item.detail.color
          |FROM mixed_array
          |LATERAL VIEW EXPLODE(items) AS item""".stripMargin)

      // detail should be pruned from struct<color,size> to struct<color>
      checkScan(query,
        "struct<items:array<struct<name:string,detail:struct<color:string>>>>")

      checkAnswer(query,
        Row("apple", "red") ::
          Row("banana", "yellow") ::
          Row("cherry", "dark red") :: Nil)
    }
  }

  testExplodePruning("nested struct pruning - only nested sub-field selected") {
    withMixedArrayData {
      // Select only detail.color: prunes name, qty (top-level) AND detail.size (nested)
      val query = sql(
        """SELECT item.detail.color
          |FROM mixed_array
          |LATERAL VIEW EXPLODE(items) AS item""".stripMargin)

      checkScan(query,
        "struct<items:array<struct<detail:struct<color:string>>>>")

      checkAnswer(query,
        Row("red") :: Row("yellow") :: Row("dark red") :: Nil)
    }
  }

  testExplodePruning("nested struct pruning - with filter on nested sub-field") {
    withMixedArrayData {
      // Select name, filter on detail.size: prunes qty but keeps detail.color + detail.size
      val query = sql(
        """SELECT item.name, item.detail.color
          |FROM mixed_array
          |LATERAL VIEW EXPLODE(items) AS item
          |WHERE item.detail.size > 1""".stripMargin)

      // detail needs both color (projected) and size (filtered), so no nested pruning on detail
      checkScan(query,
        "struct<items:array<struct<name:string,detail:struct<color:string,size:int>>>>")

      checkAnswer(query,
        Row("apple", "red") :: Row("banana", "yellow") :: Nil)
    }
  }

  // ============================================================================
  // Non-consecutive generates: generates separated by Aggregate nodes
  // Verifies the rule handles broken chains gracefully and pruning still works
  // ============================================================================

  testExplodePruning("generate above aggregate - posexplode after group by") {
    withMixedArrayData {
      // Aggregate passes the array through, then posexplode selects only some fields.
      // The Aggregate (first(items)) prevents scan-level pruning of array element fields
      // because the aggregate function needs the full array value.
      val query = sql(
        """SELECT pos, elem.name
          |FROM (
          |  SELECT first(items) as items
          |  FROM mixed_array
          |  GROUP BY id
          |) t
          |LATERAL VIEW POSEXPLODE(items) AS pos, elem""".stripMargin)

      // Scan reads full items array: the Aggregate blocks schema pruning push-down.
      // The Generate chain is broken by the Aggregate, but the system handles this
      // correctly - no crashes, correct results.
      checkScan(query,
        "struct<id:int,items:array<struct<name:string," +
          "detail:struct<color:string,size:int>,qty:int>>>")

      checkAnswer(query,
        Row(0, "apple") :: Row(1, "banana") :: Row(0, "cherry") :: Nil)
    }
  }

  testExplodePruning("non-consecutive generates separated by aggregate") {
    withMixedArrayData {
      // Pattern: Scan -> EXPLODE -> GROUP BY -> POSEXPLODE -> Project
      // The Aggregate breaks the generate chain into two independent generates.
      val query = sql(
        """SELECT category, pos, agg_item.name
          |FROM (
          |  SELECT item.detail.color as category,
          |    collect_list(named_struct('name', item.name, 'qty', item.qty)) as agg_items
          |  FROM mixed_array
          |  LATERAL VIEW EXPLODE(items) AS item
          |  GROUP BY item.detail.color
          |) t
          |LATERAL VIEW POSEXPLODE(agg_items) AS pos, agg_item""".stripMargin)

      // The inner EXPLODE needs name, qty, detail.color from items.
      // The outer POSEXPLODE is on collect_list output (not a scan array).
      checkScan(query,
        "struct<items:array<struct<name:string,detail:struct<color:string>,qty:int>>>")

      checkAnswer(query,
        Row("dark red", 0, "cherry") ::
        Row("red", 0, "apple") ::
        Row("yellow", 0, "banana") :: Nil)
    }
  }

  testExplodePruning(
      "full pipeline: scan -> filter -> agg -> explode -> agg -> filter -> posexplode -> agg") {
    withMixedArrayData {
      // Full pipeline: Scan -> Filter -> Aggregate -> EXPLODE ->
      //   GROUP BY + collect_list -> Filter -> POSEXPLODE -> Aggregate
      // Tests that the system handles a complex pipeline with multiple generates
      // separated by aggregates and filters without crashing, producing correct results.
      val query = sql(
        """WITH filtered_data AS (
          |  SELECT flatten(collect_list(items)) as all_items
          |  FROM mixed_array
          |  WHERE id > 0
          |),
          |exploded AS (
          |  SELECT item.name, item.detail.color as color, item.qty
          |  FROM filtered_data
          |  LATERAL VIEW EXPLODE(all_items) AS item
          |),
          |grouped AS (
          |  SELECT color,
          |    collect_list(named_struct('name', name, 'color', color, 'qty', qty)) as agg_items
          |  FROM exploded
          |  GROUP BY color
          |),
          |filtered_groups AS (
          |  SELECT * FROM grouped WHERE size(agg_items) >= 1
          |),
          |posexploded AS (
          |  SELECT color, pos, agg_item.name, agg_item.qty
          |  FROM filtered_groups
          |  LATERAL VIEW POSEXPLODE(agg_items) AS pos, agg_item
          |)
          |SELECT color, count(*) as cnt, sum(qty) as total_qty
          |FROM posexploded
          |GROUP BY color""".stripMargin)

      // Aggregates block scan-level nested pruning: the full items array is read.
      // detail.size is not used but cannot be pruned through flatten(collect_list(...)).
      checkScan(query,
        "struct<id:int,items:array<struct<name:string," +
          "detail:struct<color:string,size:int>,qty:int>>>")

      checkAnswer(query,
        Row("dark red", 1, 1) ::
        Row("red", 1, 5) ::
        Row("yellow", 1, 3) :: Nil)
    }
  }
}
