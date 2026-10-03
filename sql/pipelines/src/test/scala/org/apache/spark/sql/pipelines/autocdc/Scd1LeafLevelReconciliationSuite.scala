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
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd1LeafLevelReconciliationSuite extends QueryTest with SharedSparkSession {

  private val metadataColName = AutoCdcReservedNames.cdcMetadataColName

  private def dataFrameOf(schema: StructType)(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  private def changeArgs(
      ignoreNullSelection: Option[ColumnSelection],
      keys: Seq[UnqualifiedColumnName] = Seq(UnqualifiedColumnName("id"))): ChangeArgs =
    ChangeArgs(
      keys = keys,
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

  private def collapseMicrobatchRowsPerKey(
      input: DataFrame,
      sequencingType: DataType = LongType,
      keys: Seq[UnqualifiedColumnName] = Seq(UnqualifiedColumnName("id"))): DataFrame =
    Scd1LeafLevelReconciliation.collapseMicrobatchRowsPerKey(
      changeArgs = changeArgs(ignoreNullSelection = None, keys = keys),
      resolvedSequencingType = sequencingType,
      microbatchDf = input
    )

  private def metadataSchema: StructType =
    metadataSchema(LongType)

  private def metadataSchema(sequencingType: DataType): StructType =
    Scd1BatchProcessor.cdcMetadataColSchema(sequencingType)

  private def encodedPath(parts: String*): String = Scd1VersionMap.serializeKey(parts)

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

  test("collapseMicrobatchRowsPerKey selects each leaf's latest authoring event") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("left", StringType)
      .add("right", StringType)
      .add(metadataColName, metadataSchema)
    val left = encodedPath("left")
    val right = encodedPath("right")
    val input = dataFrameOf(schema)(
      Row(1, null, "right-3", Row(null, 3L, Map(left -> null, right -> 3L))),
      Row(1, "left-1", "right-1", Row(null, 1L, Map(left -> 1L, right -> 1L))),
      Row(1, "left-2", null, Row(null, 2L, Map(left -> 2L, right -> null)))
    )

    val result = collapseMicrobatchRowsPerKey(input)

    checkAnswer(
      result,
      Row(1, "left-2", "right-3", Row(null, 3L, Map(left -> 2L, right -> 3L)))
    )
  }

  test("collapseMicrobatchRowsPerKey distinguishes unauthored and authored nulls") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val value = encodedPath("value")
    val input = dataFrameOf(schema)(
      Row(1, null, Row(null, 1L, Map(value -> 1L))),
      Row(1, null, Row(null, 2L, Map(value -> null))),
      Row(1, null, Row(null, 3L, Map.empty[String, Any])),
      Row(2, null, Row(null, 1L, null)),
      Row(2, null, Row(null, 2L, Map(value -> null)))
    )

    val result = collapseMicrobatchRowsPerKey(input)

    checkAnswer(
      result.orderBy(F.col("id")),
      Seq(
        Row(1, null, Row(null, 3L, Map(value -> 1L))),
        Row(2, null, Row(null, 2L, Map(value -> 1L)))
      )
    )
  }

  test("collapseMicrobatchRowsPerKey isolates keys and treats deletes as null-valued events") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val value = encodedPath("value")
    val input = dataFrameOf(schema)(
      Row(1, "before", Row(null, 1L, Map(value -> 1L))),
      Row(2, "other", Row(null, 1L, Map(value -> 1L))),
      Row(1, null, Row(2L, null, null)),
      Row(2, null, Row(null, 2L, Map(value -> null))),
      Row(1, null, Row(null, 3L, Map(value -> null))),
      Row(1, "after", Row(null, 4L, Map(value -> 4L))),
      Row(1, null, Row(null, 5L, Map(value -> null))),
      Row(3, "before", Row(null, 1L, Map(value -> 1L))),
      Row(3, null, Row(2L, null, null)),
      Row(3, null, Row(null, 3L, Map(value -> null)))
    )

    val result = collapseMicrobatchRowsPerKey(input)

    checkAnswer(
      result.orderBy(F.col("id")),
      Seq(
        Row(1, "after", Row(2L, 5L, Map(value -> 4L))),
        Row(2, "other", Row(null, 2L, Map(value -> 1L))),
        Row(3, null, Row(2L, 3L, Map(value -> 2L)))
      )
    )
  }

  test("collapseMicrobatchRowsPerKey reconstructs nested structs from selected leaves") {
    val profileType = new StructType()
      .add("city", StringType)
      .add("zip", StringType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("profile", profileType)
      .add(metadataColName, metadataSchema)
    val city = encodedPath("profile", "city")
    val zip = encodedPath("profile", "zip")
    val input = dataFrameOf(schema)(
      Row(1, Row("San Francisco", "94107"),
        Row(null, 1L, Map(city -> 1L, zip -> 1L))),
      Row(1, Row(null, "10001"), Row(null, 2L, Map(city -> null, zip -> 2L))),
      Row(2, null, Row(null, 1L, Map(city -> null, zip -> null)))
    )

    val result = collapseMicrobatchRowsPerKey(input)

    checkAnswer(
      result.orderBy(F.col("id")),
      Seq(
        Row(1, Row("San Francisco", "10001"),
          Row(null, 2L, Map(city -> 1L, zip -> 2L))),
        Row(2, null, Row(null, 1L, Map(city -> null, zip -> null)))
      )
    )
  }

  test("collapseMicrobatchRowsPerKey materializes a dense map for row-level input") {
    val locationType = new StructType()
      .add("city", StringType, nullable = false)
    val profileType = new StructType()
      .add("location", locationType, nullable = false)
    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("value", StringType, nullable = false)
      .add("profile", profileType)
      .add(metadataColName, metadataSchema, nullable = false)
    val input = dataFrameOf(schema)(
      Row(1, "value", Row(Row("city")), Row(null, 1L, null))
    )

    val result = collapseMicrobatchRowsPerKey(input)

    checkAnswer(
      result,
      Row(
        1,
        "value",
        Row(Row("city")),
        Row(
          null,
          1L,
          Map(
            encodedPath("value") -> 1L,
            encodedPath("profile", "location", "city") -> 1L
          )
        )
      )
    )
  }

  test("collapseMicrobatchRowsPerKey rebuilds unauthored non-nullable structs with metadata") {
    val profileMetadata = new MetadataBuilder().putString("description", "profile").build()
    val cityMetadata = new MetadataBuilder().putString("description", "city").build()
    val profileType =
      StructType(Seq(StructField("city", StringType, nullable = true, cityMetadata)))
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("profile", profileType, nullable = false, profileMetadata),
      StructField(metadataColName, metadataSchema)
    ))
    val city = encodedPath("profile", "city")
    val input =
      dataFrameOf(schema)(Row(1, Row(null), Row(null, 1L, Map[String, Any](city -> null))))

    val result = collapseMicrobatchRowsPerKey(input)

    val resultProfile = result.schema("profile")
    val resultCity = resultProfile.dataType.asInstanceOf[StructType]("city")
    assert(!resultProfile.nullable)
    assert(resultProfile.metadata == profileMetadata)
    assert(resultCity.metadata == cityMetadata)
    checkAnswer(result, Row(1, Row(null), Row(null, 1L, Map[String, Any](city -> null))))
  }

  test("collapseMicrobatchRowsPerKey handles quoted paths and non-struct complex leaves") {
    val profileType = new StructType().add("city", StringType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("profile.city", StringType)
      .add("profile", profileType)
      .add("tags", ArrayType(StringType))
      .add("properties", MapType(StringType, StringType))
      .add(metadataColName, metadataSchema)
    val dottedCity = encodedPath("profile.city")
    val nestedCity = encodedPath("profile", "city")
    val tags = encodedPath("tags")
    val properties = encodedPath("properties")
    val firstVersionMap = Map(dottedCity -> 1L, nestedCity -> 1L, tags -> 1L, properties -> 1L)
    val secondVersionMap = Map[String, Any](
      dottedCity -> 2L, nestedCity -> null, tags -> 2L, properties -> null)
    val input = dataFrameOf(schema)(
      Row(1, "dotted-1", Row("nested-1"), Seq("tag-1"), Map("key" -> "properties-1"),
        Row(null, 1L, firstVersionMap)),
      Row(1, "dotted-2", Row("nested-2"), Seq("tag-2"), Map("key" -> "properties-2"),
        Row(null, 2L, secondVersionMap))
    )

    val result = collapseMicrobatchRowsPerKey(input)
    val expectedVersionMap = Map(dottedCity -> 2L, nestedCity -> 1L, tags -> 2L, properties -> 1L)

    checkAnswer(
      result,
      Row(1, "dotted-2", Row("nested-1"), Seq("tag-2"), Map("key" -> "properties-1"),
        Row(null, 2L, expectedVersionMap))
    )
  }

  test("collapseMicrobatchRowsPerKey orders composite sequencing values") {
    val sequencingType = new StructType()
      .add("timestamp", LongType, nullable = false)
      .add("tieBreaker", IntegerType, nullable = false)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema(sequencingType))
    val value = encodedPath("value")
    val firstSequence = Row(10L, 1)
    val secondSequence = Row(10L, 2)
    val input = dataFrameOf(schema)(
      Row(1, "first", Row(null, firstSequence, Map(value -> firstSequence))),
      Row(1, "second", Row(null, secondSequence, Map(value -> secondSequence)))
    )

    val result = collapseMicrobatchRowsPerKey(input, sequencingType)

    checkAnswer(result, Row(1, "second", Row(null, secondSequence, Map(value -> secondSequence))))
  }

  test("collapseMicrobatchRowsPerKey uses the session resolver to identify key columns") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val schema = new StructType()
        .add("ID", IntegerType)
        .add("value", StringType)
        .add(metadataColName, metadataSchema)
      val value = encodedPath("value")
      val input = dataFrameOf(schema)(
        Row(1, "first", Row(null, 1L, Map(value -> 1L))),
        Row(1, "second", Row(null, 2L, Map(value -> 2L)))
      )

      val result = collapseMicrobatchRowsPerKey(input)

      assert(result.schema.fieldNames.toSeq == Seq("ID", "value", metadataColName))
      checkAnswer(result, Row(1, "second", Row(null, 2L, Map(value -> 2L))))
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val schema = new StructType()
        .add("ID", IntegerType)
        .add("id", StringType)
        .add(metadataColName, metadataSchema)
      val lowerCaseId = encodedPath("id")
      val input = dataFrameOf(schema)(
        Row(1, "first", Row(null, 1L, Map(lowerCaseId -> 1L))),
        Row(1, "second", Row(null, 2L, Map(lowerCaseId -> 2L)))
      )

      val result =
        collapseMicrobatchRowsPerKey(input, keys = Seq(UnqualifiedColumnName("ID")))

      checkAnswer(result, Row(1, "second", Row(null, 2L, Map(lowerCaseId -> 2L))))
    }
  }

  test("collapseMicrobatchRowsPerKey supports schemas with no user-data leaves") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema)
    val input = dataFrameOf(schema)(Row(1, Row(null, 1L, null)))

    val result = collapseMicrobatchRowsPerKey(input)

    checkAnswer(result, Row(1, Row(null, 1L, Map.empty[String, Long])))
  }

  test("collapseMicrobatchRowsPerKey preserves an empty microbatch") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val input = dataFrameOf(schema)()

    val result = collapseMicrobatchRowsPerKey(input)

    assert(result.schema.fieldNames.sameElements(schema.fieldNames))
    assert(result.isEmpty)
  }
}
