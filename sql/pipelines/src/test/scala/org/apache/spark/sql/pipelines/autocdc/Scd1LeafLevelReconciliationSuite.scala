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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.sql.{functions => F, QueryTest, Row}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd1LeafLevelReconciliationSuite extends QueryTest with SharedSparkSession {

  private val metadataColName = AutoCdcReservedNames.cdcMetadataColName

  private def dataFrameOf(schema: StructType)(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  private def changeArgs(ignoreNullSelection: Option[ColumnSelection]): ChangeArgs =
    ChangeArgs(
      keys = Seq(UnqualifiedColumnName("id")),
      sequencing = F.col("seq"),
      storedAsScdType = ScdType.Type1,
      ignoreNullSelection = ignoreNullSelection
    )

  private def extendMicrobatchRowsWithVersionMap(
      input: DataFrame,
      ignoreNullSelection: Option[ColumnSelection]): DataFrame =
    Scd1LeafLevelReconciliation.extendMicrobatchRowsWithVersionMap(
      changeArgs = changeArgs(ignoreNullSelection),
      resolvedSequencingType = LongType,
      alignedDf = input
    )

  private def metadataSchema: StructType =
    Scd1BatchProcessor.cdcMetadataColSchema(LongType)

  private def encodedPath(parts: String*): String = QuotingUtils.quoteNameParts(parts)

  test("extendMicrobatchRowsWithVersionMap is a no-op when ignore-null is disabled") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val existingMap = Map(encodedPath("value") -> 5L)
    val input = dataFrameOf(schema)(
      Row(1, "value", Row(null, 5L, existingMap))
    )

    val result = extendMicrobatchRowsWithVersionMap(
      input = input,
      ignoreNullSelection = None
    )

    assert(result eq input)
    checkAnswer(result, input.collect().toSeq)
  }

  test("extendMicrobatchRowsWithVersionMap populates upserts without changing other CDC " +
    "metadata fields") {
    val profileType = new StructType()
      .add("city", StringType)
      .add("age", IntegerType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("selectedNull", StringType)
      .add("selectedValue", StringType)
      .add("unselectedNull", StringType)
      .add("profile", profileType)
      .add(metadataColName, metadataSchema)
    val input = dataFrameOf(schema)(
      Row(1, null, "selected", null, Row(null, 42), Row(null, 10L, null)),
      Row(2, null, null, null, Row(null, null), Row(20L, null, null)),
      Row(3, "authored", null, null, Row("New York", null), Row(null, 30L, null))
    )
    val ignoreNullSelection = ColumnSelection.IncludeColumns(Seq(
      UnqualifiedColumnName("selectedNull"),
      UnqualifiedColumnName("selectedValue"),
      UnqualifiedColumnName("profile")
    ))

    val result = extendMicrobatchRowsWithVersionMap(
      input = input,
      ignoreNullSelection = Some(ignoreNullSelection)
    )

    val expectedUpsertMap = Map[String, Any](
      encodedPath("selectedNull") -> null,
      encodedPath("selectedValue") -> 10L,
      encodedPath("unselectedNull") -> 10L,
      encodedPath("profile", "city") -> null,
      encodedPath("profile", "age") -> 10L
    )
    val expectedSecondUpsertMap = Map[String, Any](
      encodedPath("selectedNull") -> 30L,
      encodedPath("selectedValue") -> null,
      encodedPath("unselectedNull") -> 30L,
      encodedPath("profile", "city") -> 30L,
      encodedPath("profile", "age") -> null
    )
    checkAnswer(
      result.select(F.col(metadataColName)),
      Seq(
        Row(Row(null, 10L, expectedUpsertMap)),
        Row(Row(20L, null, null)),
        Row(Row(null, 30L, expectedSecondUpsertMap))
      )
    )
  }

  test("alignMicrobatchToTargetSchema preserves target shape and source-only columns") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val targetProfileType = new StructType()
        .add("city", StringType)
        .add("zip", StringType)
      val targetSchema = new StructType()
        .add("id", IntegerType)
        .add("name", StringType)
        .add("profile", targetProfileType)
        .add("targetOnly", StringType)
      val target = dataFrameOf(targetSchema)(
        Row(999, "target", Row("target city", "target zip"), "target only")
      )

      val sourceProfileType = new StructType().add("City", StringType)
      val projectedSchema = new StructType()
        .add("newColumn", StringType)
        .add("id", IntegerType)
        .add("Name", StringType)
        .add("profile", sourceProfileType)
      val projected = dataFrameOf(projectedSchema)(
        Row("new", 1, "Alice", Row("San Francisco"))
      )

      val result = Scd1LeafLevelReconciliation.alignMicrobatchToTargetSchema(
        microbatchDf = projected,
        targetTableDf = target
      )

      assert(result.schema.fieldNames.toSeq ==
        Seq("id", "name", "profile", "targetOnly", "newColumn"))
      assert(result.schema("profile").dataType == targetProfileType)
      checkAnswer(
        result,
        Row(1, "Alice", Row("San Francisco", null), null, "new")
      )
    }
  }
}
