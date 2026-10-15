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
      keys: Seq[UnqualifiedColumnName] = Seq(UnqualifiedColumnName("id")),
      columnSelection: Option[ColumnSelection] = None): ChangeArgs =
    ChangeArgs(
      keys = keys,
      sequencing = F.col("seq"),
      storedAsScdType = ScdType.Type1,
      columnSelection = columnSelection,
      ignoreNullSelection = ignoreNullSelection
    )

  private def extendMicrobatchRowsWithVersionMap(
      input: DataFrame,
      ignoreNullSelection: Option[ColumnSelection],
      unalignedMicrobatchSchema: Option[StructType] = None): DataFrame =
    Scd1LeafLevelReconciliation.extendMicrobatchRowsWithVersionMap(
      changeArgs = changeArgs(ignoreNullSelection),
      resolvedSequencingType = LongType,
      alignedMicrobatchDf = input,
      unalignedMicrobatchSchema = unalignedMicrobatchSchema.getOrElse(input.schema)
    )

  private def collapseMicrobatchRowsPerKey(
      input: DataFrame,
      sequencingType: DataType = LongType,
      keys: Seq[UnqualifiedColumnName] = Seq(UnqualifiedColumnName("id")),
      auxiliaryTable: Option[DataFrame] = None,
      ignoreNullSelection: Option[ColumnSelection] =
        Some(ColumnSelection.ExcludeColumns(Seq.empty))): DataFrame = {
    val emptyAuxiliaryTable = input.select(
      (keys.map(key => F.col(key.quoted)) :+ F.col(metadataColName)): _*
    ).limit(0)
    Scd1LeafLevelReconciliation.collapseMicrobatchRowsPerKey(
      changeArgs = changeArgs(ignoreNullSelection = ignoreNullSelection, keys = keys),
      resolvedSequencingType = sequencingType,
      microbatchDf = input,
      auxiliaryTableDf = auxiliaryTable.getOrElse(emptyAuxiliaryTable)
    )
  }

  private def metadataSchema: StructType =
    metadataSchema(LongType)

  private def metadataSchema(sequencingType: DataType): StructType =
    Scd1BatchProcessor.cdcMetadataColSchema(sequencingType)

  private def encodedPath(parts: String*): String = Scd1VersionMap.serializeKey(parts)

  test("reconcileMicrobatch returns selected target-aligned rows after applying tombstones") {
    val inputSchema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("city", StringType)
      .add("seq", LongType)
      .add("sourceOnly", StringType)
    val targetSchema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("city", StringType)
      .add("targetOnly", StringType)
      .add(metadataColName, metadataSchema)
    val auxiliarySchema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema)
    val input = dataFrameOf(inputSchema)(
      Row(1, "Alice", null, 1L, "excluded"),
      Row(1, null, "San Francisco", 3L, "excluded"),
      Row(2, "Stale", null, 1L, "excluded")
    )
    val target = dataFrameOf(targetSchema)()
    val auxiliary = dataFrameOf(auxiliarySchema)(
      Row(1, Row(2L, null, null)),
      Row(2, Row(2L, null, null))
    )
    val name = encodedPath("name")
    val city = encodedPath("city")
    val targetOnly = encodedPath("targetOnly")
    val args = changeArgs(
      ignoreNullSelection = Some(ColumnSelection.IncludeColumns(Seq(
        UnqualifiedColumnName("name"), UnqualifiedColumnName("city"),
        UnqualifiedColumnName("targetOnly")
      ))),
      columnSelection = Some(ColumnSelection.ExcludeColumns(
        Seq(UnqualifiedColumnName("seq"), UnqualifiedColumnName("sourceOnly"))))
    )

    val result = Scd1LeafLevelReconciliation.reconcileMicrobatch(
      changeArgs = args,
      resolvedSequencingType = LongType,
      validatedBatchDf = input,
      targetTableDf = target,
      auxiliaryTableDf = auxiliary
    )

    assert(result.schema.fieldNames.sameElements(targetSchema.fieldNames))
    val expectedVersionMap = Map(name -> 2L, city -> 3L, targetOnly -> 2L)
    checkAnswer(result, Row(1, null, "San Francisco", null, Row(null, 3L, expectedVersionMap)))
  }

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

  test("extendMicrobatchRowsWithVersionMap leaves the leaves that alignment added unauthored") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val schema = new StructType()
        .add("id", IntegerType)
        .add("selected", StringType)
        .add("carried", StringType)
        .add("omitted", StringType)
        .add("profile", new StructType()
          .add("city", StringType)
          .add("zip", StringType))
        .add(metadataColName, metadataSchema)
      val unalignedMicrobatchSchema = new StructType()
        .add("ID", IntegerType)
        .add("Selected", StringType)
        .add("Carried", StringType)
        .add("profile", new StructType()
          .add("City", StringType))
        .add(metadataColName, metadataSchema)
      val input = dataFrameOf(schema)(
        Row(1, "value", null, null, Row(null, null), Row(null, 10L, null))
      )

      val result = extendMicrobatchRowsWithVersionMap(
        input = input,
        ignoreNullSelection =
          Some(ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("selected")))),
        unalignedMicrobatchSchema = Some(unalignedMicrobatchSchema)
      )

      val expectedVersionMap = Map[String, Any](
        encodedPath("selected") -> 10L,
        encodedPath("carried") -> 10L,
        encodedPath("omitted") -> null,
        encodedPath("profile", "city") -> 10L,
        encodedPath("profile", "zip") -> null
      )
      checkAnswer(
        result.select(F.col(metadataColName)),
        Row(Row(null, 10L, expectedVersionMap)))
    }
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
        Row(1, "after", Row(null, 5L, Map(value -> 4L))),
        Row(2, "other", Row(null, 2L, Map(value -> 1L))),
        Row(3, null, Row(null, 3L, Map(value -> 2L)))
      )
    )
  }

  test("collapseMicrobatchRowsPerKey collapses keys whose latest event is a delete into " +
    "row-wide deletes") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val value = encodedPath("value")
    val input = dataFrameOf(schema)(
      Row(1, "first", Row(null, 1L, Map(value -> 1L))),
      Row(1, null, Row(3L, null, null)),
      Row(1, "second", Row(null, 2L, Map(value -> 2L))),
      Row(2, null, Row(2L, null, null))
    )

    val result = collapseMicrobatchRowsPerKey(input)

    checkAnswer(
      result.orderBy(F.col("id")),
      Seq(
        Row(1, null, Row(3L, null, null)),
        Row(2, null, Row(2L, null, null))
      )
    )
  }

  test("collapseMicrobatchRowsPerKey collapses keys into upserts on delete and upsert " +
    "sequencing ties") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val value = encodedPath("value")
    val input = dataFrameOf(schema)(
      Row(1, "upsert", Row(null, 2L, Map(value -> 2L))),
      Row(1, null, Row(2L, null, null))
    )

    val result = collapseMicrobatchRowsPerKey(input)

    // Leaf selection between events with equal sequencing values is undefined, so only the CDC
    // metadata is deterministic.
    checkAnswer(result.select(F.col(metadataColName)), Row(Row(null, 2L, Map(value -> 2L))))
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
        Row(2, Row(null, null), Row(null, 1L, Map(city -> null, zip -> null)))
      )
    )
  }

  test("collapseMicrobatchRowsPerKey rebuilds selected structs from null leaves") {
    val locationType = new StructType()
      .add("city", StringType)
      .add("zip", StringType)
    val profileType = new StructType()
      .add("location", locationType)
      .add("name", StringType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("profile", profileType)
      .add(metadataColName, metadataSchema)
    val city = encodedPath("profile", "location", "city")
    val zip = encodedPath("profile", "location", "zip")
    val name = encodedPath("profile", "name")
    val unauthored = Map[String, Any](city -> null, zip -> null, name -> null)
    val input = dataFrameOf(schema)(
      // Structs provided with only null leaves author nothing, at any depth.
      Row(1, Row(Row(null, null), null), Row(null, 1L, unauthored)),
      Row(2, Row(Row(null, null), "Ada"), Row(null, 1L, unauthored + (name -> 1L))),
      Row(3, Row(Row(null, "94107"), null), Row(null, 1L, unauthored + (zip -> 1L))),
      // A microbatch delete authors null for every leaf.
      Row(4, Row(Row("SF", null), null), Row(null, 1L, unauthored + (city -> 1L))),
      Row(4, null, Row(2L, null, null)),
      Row(4, Row(Row(null, null), "Ada"), Row(null, 3L, unauthored + (name -> 3L)))
    )

    val result = collapseMicrobatchRowsPerKey(input)

    checkAnswer(
      result.orderBy(F.col("id")),
      Seq(
        Row(1, Row(Row(null, null), null), Row(null, 1L, unauthored)),
        Row(2, Row(Row(null, null), "Ada"), Row(null, 1L, unauthored + (name -> 1L))),
        Row(3, Row(Row(null, "94107"), null), Row(null, 1L, unauthored + (zip -> 1L))),
        Row(4, Row(Row(null, null), "Ada"),
          Row(null, 3L, Map(city -> 2L, zip -> 2L, name -> 3L))))
    )
  }

  test("collapseMicrobatchRowsPerKey reconciles columns outside the ignore-null selection whole") {
    val locationType = new StructType()
      .add("city", StringType)
      .add("zip", StringType)
    val profileType = new StructType()
      .add("location", locationType)
      .add("name", StringType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("profile", profileType)
      .add("nickname", StringType)
      .add(metadataColName, metadataSchema)
    val auxiliarySchema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema)
    val city = encodedPath("profile", "location", "city")
    val zip = encodedPath("profile", "location", "zip")
    val name = encodedPath("profile", "name")
    val nickname = encodedPath("nickname")
    // Every upsert authors the unselected profile leaves, but only a non-null nickname.
    def versionMap(sequence: Long, nicknameAuthoredAt: Any): Map[String, Any] =
      Map(city -> sequence, zip -> sequence, name -> sequence, nickname -> nicknameAuthoredAt)
    val input = dataFrameOf(schema)(
      // The winning upsert's profile is kept whole, including structs of nulls.
      Row(1, Row(Row(null, null), null), "Al", Row(null, 1L, versionMap(1L, 1L))),
      // A later null profile replaces an earlier non-null one.
      Row(2, Row(Row("SF", "94107"), "Ada"), "Bo", Row(null, 1L, versionMap(1L, 1L))),
      Row(2, null, null, Row(null, 2L, versionMap(2L, null))),
      Row(3, null, null, Row(null, 1L, versionMap(1L, null))),
      Row(3, Row(Row(null, "94107"), null), null, Row(null, 2L, versionMap(2L, null))),
      // An upsert after a delete authors the whole profile, but not its null nickname.
      Row(4, Row(Row("SF", null), "Ada"), null, Row(null, 1L, versionMap(1L, null))),
      Row(4, null, null, Row(2L, null, null)),
      Row(4, Row(null, null), null, Row(null, 3L, versionMap(3L, null))),
      // A tombstone at 5 authors the nickname last authored before it, but never the profile,
      // which the key's winning upsert always authors.
      Row(5, Row(Row("LA", null), null), "Cy", Row(null, 4L, versionMap(4L, 4L))),
      Row(5, Row(Row("NY", null), null), null, Row(null, 6L, versionMap(6L, null)))
    )
    val auxiliary = dataFrameOf(auxiliarySchema)(Row(5, Row(5L, null, null)))

    val result = collapseMicrobatchRowsPerKey(
      input,
      auxiliaryTable = Some(auxiliary),
      ignoreNullSelection =
        Some(ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("nickname")))))

    checkAnswer(
      result.orderBy(F.col("id")),
      Seq(
        Row(1, Row(Row(null, null), null), "Al", Row(null, 1L, versionMap(1L, 1L))),
        Row(2, null, "Bo", Row(null, 2L, versionMap(2L, 1L))),
        Row(3, Row(Row(null, "94107"), null), null, Row(null, 2L, versionMap(2L, null))),
        Row(4, Row(null, null), null, Row(null, 3L, versionMap(3L, 2L))),
        Row(5, Row(Row("NY", null), null), null, Row(null, 6L, versionMap(6L, 5L))))
    )
  }

  test("collapseMicrobatchRowsPerKey reconciles every column whole when ignore-null is disabled") {
    val cityMetadata = new MetadataBuilder().putString("description", "city").build()
    val profileMetadata = new MetadataBuilder().putString("description", "profile").build()
    val profileType = new StructType()
      .add("city", StringType, nullable = true, cityMetadata)
      .add("zip", StringType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("profile", profileType, nullable = true, profileMetadata)
      .add("nickname", StringType)
      .add(metadataColName, metadataSchema)
    // Without ignore-null, version maps are null and every upsert authors every column.
    val input = dataFrameOf(schema)(
      Row(1, Row("SF", null), "Al", Row(null, 1L, null)),
      Row(1, Row(null, null), null, Row(null, 2L, null))
    )

    val result = collapseMicrobatchRowsPerKey(input, ignoreNullSelection = None)

    val expectedVersionMap = Map(
      encodedPath("profile", "city") -> 2L,
      encodedPath("profile", "zip") -> 2L,
      encodedPath("nickname") -> 2L)
    assert(result.schema("profile") == schema("profile"))
    checkAnswer(result, Row(1, Row(null, null), null, Row(null, 2L, expectedVersionMap)))
  }

  test("collapseMicrobatchRowsPerKey materializes version-map entries for row-level input") {
    val cityMetadata = new MetadataBuilder().putString("description", "city").build()
    val locationMetadata = new MetadataBuilder().putString("description", "location").build()
    val profileMetadata = new MetadataBuilder().putString("description", "profile").build()
    val locationType = StructType(
      Seq(StructField("city", StringType, nullable = false, cityMetadata)))
    val profileType = StructType(
      Seq(StructField("location", locationType, nullable = false, locationMetadata)))
    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("value", StringType, nullable = false)
      .add("profile", profileType, nullable = true, profileMetadata)
      .add(metadataColName, metadataSchema, nullable = false)
    val input = dataFrameOf(schema)(Row(1, "value", Row(Row("city")), Row(null, 1L, null)))

    val result = collapseMicrobatchRowsPerKey(input)
    val expectedVersionMap = Map(
      encodedPath("value") -> 1L, encodedPath("profile", "location", "city") -> 1L)

    assert(result.schema("profile") == schema("profile"))
    checkAnswer(
      result,
      Row(1, "value", Row(Row("city")), Row(null, 1L, expectedVersionMap))
    )
  }

  test("collapseMicrobatchRowsPerKey carries the latest delete event's values on net deletes") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType, nullable = false)
      .add("value", StringType, nullable = false)
      .add(metadataColName, metadataSchema)
    val authored = Map(encodedPath("name") -> 1L, encodedPath("value") -> 1L)
    val input = dataFrameOf(schema)(
      Row(1, "name-1", "value-1", Row(null, 1L, authored)),
      Row(1, "name-2", "value-2", Row(2L, null, null)),
      Row(1, "name-3", "value-3", Row(3L, null, null)))

    // `name` is reconciled leaf by leaf and `value` as a whole column. Both are non-nullable, so
    // the nulls that the deletes authored cannot be carried.
    val result = collapseMicrobatchRowsPerKey(
      input,
      ignoreNullSelection =
        Some(ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("name")))))

    assert(result.schema("name") == schema("name"))
    assert(result.schema("value") == schema("value"))
    checkAnswer(result, Row(1, "name-3", "value-3", Row(3L, null, null)))
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

  test("collapseMicrobatchRowsPerKey applies composite-sequence tombstones to older leaves") {
    val sequencingType = new StructType()
      .add("timestamp", LongType, nullable = false)
      .add("tieBreaker", IntegerType, nullable = false)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("left", StringType)
      .add("right", StringType)
      .add(metadataColName, metadataSchema(sequencingType))
    val auxiliarySchema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema(sequencingType))
    val left = encodedPath("left")
    val right = encodedPath("right")
    val firstSequence = Row(10L, 1)
    val tombstoneSequence = Row(10L, 2)
    val secondSequence = Row(10L, 3)
    val input = dataFrameOf(schema)(
      Row(1, "left", null,
        Row(null, firstSequence, Map[String, Any](left -> firstSequence, right -> null))),
      Row(1, null, "right",
        Row(null, secondSequence, Map[String, Any](left -> null, right -> secondSequence)))
    )
    val auxiliary = dataFrameOf(auxiliarySchema)(
      Row(1, Row(tombstoneSequence, null, null)))

    val result = collapseMicrobatchRowsPerKey(
      input, sequencingType = sequencingType, auxiliaryTable = Some(auxiliary))

    val expectedVersionMap = Map(left -> tombstoneSequence, right -> secondSequence)
    checkAnswer(
      result,
      Row(1, null, "right", Row(null, secondSequence, expectedVersionMap))
    )
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

  test("collapseMicrobatchRowsPerKey drops upserts older than tombstones and keeps ties, newer " +
    "upserts, and unmatched keys") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val auxiliarySchema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema)
    val value = encodedPath("value")
    val input = dataFrameOf(schema)(
      Row(1, "stale", Row(null, 4L, Map(value -> 4L))),
      Row(2, "tie", Row(null, 5L, Map(value -> 5L))),
      Row(3, "unchanged", Row(null, 3L, Map(value -> 3L))),
      Row(4, "newer", Row(null, 6L, Map(value -> 6L)))
    )
    val auxiliary = dataFrameOf(auxiliarySchema)(
      Row(1, Row(5L, null, null)),
      Row(2, Row(5L, null, null)),
      Row(4, Row(5L, null, null))
    )

    val result = collapseMicrobatchRowsPerKey(input, auxiliaryTable = Some(auxiliary))

    checkAnswer(
      result.orderBy(F.col("id")),
      Seq(
        Row(2, "tie", Row(null, 5L, Map(value -> 5L))),
        Row(3, "unchanged", Row(null, 3L, Map(value -> 3L))),
        Row(4, "newer", Row(null, 6L, Map(value -> 6L)))
      )
    )
  }

  test("collapseMicrobatchRowsPerKey keeps net deletes after tombstones and drops keys whose " +
    "events are all older") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val auxiliarySchema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema)
    val value = encodedPath("value")
    val input = dataFrameOf(schema)(
      Row(1, "newer delete", Row(6L, null, null)),
      Row(2, "older upsert", Row(null, 3L, Map(value -> 3L))),
      Row(2, "older delete", Row(4L, null, null))
    )
    val auxiliary = dataFrameOf(auxiliarySchema)(
      Row(1, Row(5L, null, null)),
      Row(2, Row(5L, null, null))
    )

    val result = collapseMicrobatchRowsPerKey(input, auxiliaryTable = Some(auxiliary))

    checkAnswer(result, Row(1, "newer delete", Row(6L, null, null)))
  }

  test("collapseMicrobatchRowsPerKey lets a microbatch delete win against a persisted " +
    "tombstone") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val auxiliarySchema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema)
    val value = encodedPath("value")
    val input = dataFrameOf(schema)(
      Row(1, null, Row(6L, null, null)),
      Row(1, null, Row(null, 7L, Map(value -> null)))
    )
    val auxiliary = dataFrameOf(auxiliarySchema)(Row(1, Row(5L, null, null)))

    val result = collapseMicrobatchRowsPerKey(input, auxiliaryTable = Some(auxiliary))

    checkAnswer(result, Row(1, null, Row(null, 7L, Map(value -> 6L))))
  }

  test("collapseMicrobatchRowsPerKey matches row-level tombstone filtering when ignore-null is " +
    "disabled") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val auxiliarySchema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema)
    val input = dataFrameOf(schema)(
      Row(1, "stale", Row(null, 4L, null)),
      Row(2, "tie", Row(null, 5L, null)),
      Row(3, "newer delete", Row(6L, null, null)),
      Row(4, "no tombstone", Row(null, 1L, null))
    )
    val auxiliary = dataFrameOf(auxiliarySchema)(
      Row(1, Row(5L, null, null)),
      Row(2, Row(5L, null, null)),
      Row(3, Row(5L, null, null))
    )

    val rowLevelResult = Scd1RowLevelReconciliation.applyTombstonesToMicrobatch(
      changeArgs = changeArgs(ignoreNullSelection = None),
      microbatchDf = input,
      auxiliaryTableDf = auxiliary
    )
    val leafLevelResult = collapseMicrobatchRowsPerKey(
      input,
      auxiliaryTable = Some(auxiliary),
      ignoreNullSelection = None
    )

    checkAnswer(
      leafLevelResult.select("id", "value"),
      rowLevelResult.select("id", "value").collect().toSeq
    )
  }

  test("collapseMicrobatchRowsPerKey matches composite and quoted tombstone keys") {
    val keys = Seq(UnqualifiedColumnName("region"), UnqualifiedColumnName("`user.id`"))
    val schema = new StructType()
      .add("region", StringType)
      .add("user.id", IntegerType)
      .add("value", StringType)
      .add(metadataColName, metadataSchema)
    val auxiliarySchema = new StructType()
      .add("region", StringType)
      .add("user.id", IntegerType)
      .add(metadataColName, metadataSchema)
    val value = encodedPath("value")
    val input = dataFrameOf(schema)(
      Row("US", 1, "stale", Row(null, 4L, Map(value -> 4L))),
      Row("US", 2, "newer", Row(null, 6L, Map(value -> 6L)))
    )
    val auxiliary = dataFrameOf(auxiliarySchema)(
      Row("US", 1, Row(5L, null, null)),
      Row("US", 2, Row(5L, null, null))
    )

    val result = collapseMicrobatchRowsPerKey(
      input, keys = keys, auxiliaryTable = Some(auxiliary))

    checkAnswer(result, Row("US", 2, "newer", Row(null, 6L, Map(value -> 6L))))
  }

  test("collapseMicrobatchRowsPerKey sets leaves not written after a tombstone to null at its " +
    "sequence") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("before", StringType)
      .add("after", StringType)
      .add("neverAuthored", StringType)
      .add(metadataColName, metadataSchema)
    val auxiliarySchema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema)
    val before = encodedPath("before")
    val after = encodedPath("after")
    val neverAuthored = encodedPath("neverAuthored")
    val input = dataFrameOf(schema)(
      Row(1, null, null, null, Row(4L, null, null)),
      Row(
        1, null, "after", null,
        Row(null, 6L, Map(before -> null, after -> 6L, neverAuthored -> null)))
    )
    val auxiliary = dataFrameOf(auxiliarySchema)(Row(1, Row(5L, null, null)))

    val result = collapseMicrobatchRowsPerKey(input, auxiliaryTable = Some(auxiliary))

    checkAnswer(
      result,
      Row(
        1, null, "after", null,
        Row(null, 6L, Map(before -> 5L, after -> 6L, neverAuthored -> 5L)))
    )
  }

  test("collapseMicrobatchRowsPerKey applies tombstones to all leaf types") {
    val profileType = new StructType()
      .add("city", StringType)
      .add("zip", StringType)
    val schema = new StructType()
      .add("id", IntegerType)
      .add("profile", profileType)
      .add("profile.city", StringType)
      .add("tags", ArrayType(StringType))
      .add(metadataColName, metadataSchema)
    val auxiliarySchema = new StructType()
      .add("id", IntegerType)
      .add(metadataColName, metadataSchema)
    val city = encodedPath("profile", "city")
    val zip = encodedPath("profile", "zip")
    val dottedCity = encodedPath("profile.city")
    val tags = encodedPath("tags")
    val input = dataFrameOf(schema)(
      Row(
        1, Row("old city", null), "old dotted city", null,
        Row(null, 4L, Map(city -> 4L, zip -> null, dottedCity -> 4L, tags -> null))),
      Row(
        1, Row(null, "new zip"), null, Seq("new tag"),
        Row(null, 6L, Map(city -> null, zip -> 6L, dottedCity -> null, tags -> 6L))),
      Row(
        2, null, null, null,
        Row(null, 6L, Map(city -> null, zip -> null, dottedCity -> null, tags -> null)))
    )
    val auxiliary = dataFrameOf(auxiliarySchema)(
      Row(1, Row(5L, null, null)),
      Row(2, Row(5L, null, null))
    )

    val result = collapseMicrobatchRowsPerKey(input, auxiliaryTable = Some(auxiliary))
    val tombstoneOnlyMap = Map(city -> 5L, zip -> 5L, dottedCity -> 5L, tags -> 5L)

    checkAnswer(
      result.orderBy(F.col("id")),
      Seq(
        Row(
          1, Row(null, "new zip"), null, Seq("new tag"),
          Row(null, 6L, Map(city -> 5L, zip -> 6L, dottedCity -> 5L, tags -> 6L))),
        Row(2, Row(null, null), null, null, Row(null, 6L, tombstoneOnlyMap))
      )
    )
  }
}
