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
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd1VersionMapSuite extends QueryTest with SharedSparkSession {

  private def encodedPath(parts: String*): String = Scd1VersionMap.serializeKey(parts)

  private def dataFrameOf(schema: StructType)(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  test("mapType uses the supplied sequencing type and permits null values") {
    val sequencingType = new StructType()
      .add("timestamp", LongType)
      .add("tieBreaker", IntegerType)

    assert(Scd1VersionMap.mapType(sequencingType) ==
      MapType(StringType, sequencingType, valueContainsNull = true))
  }

  test("buildVersionMap records every leaf with the expected sequencing clock") {
    val userDataSchema = new StructType()
      .add("selectedNull", StringType)
      .add("selectedValue", StringType)
      .add("unselectedNull", StringType)
      .add("unselectedValue", StringType)
    val inputSchema = StructType(userDataSchema.fields :+ StructField("seq", IntegerType))
    val input = dataFrameOf(inputSchema)(
      Row(null, "selected", null, "unselected", 10)
    )
    val ignoreNullSelection = ColumnSelection.IncludeColumns(Seq(
      UnqualifiedColumnName("selectedNull"),
      UnqualifiedColumnName("selectedValue")
    ))

    val result = input.select(
      Scd1VersionMap.buildVersionMap(
        schema = userDataSchema,
        ignoreNullSelection = ignoreNullSelection,
        upsertSequence = F.col("seq"),
        sequencingType = IntegerType,
        resolver = caseSensitiveResolution
      ).as("versionMap")
    )

    assert(result.schema("versionMap").dataType == Scd1VersionMap.mapType(IntegerType))
    checkAnswer(result, Row(Map[String, Any](
      encodedPath("selectedNull") -> null,
      encodedPath("selectedValue") -> 10,
      encodedPath("unselectedNull") -> 10,
      encodedPath("unselectedValue") -> 10
    )))
  }

  test("buildVersionMap supports exclude-list ignore-null selections") {
    val userDataSchema = new StructType()
      .add("ignoredNull", StringType)
      .add("authoredNull", StringType)
    val inputSchema = StructType(userDataSchema.fields :+ StructField("seq", LongType))
    val input = dataFrameOf(inputSchema)(Row(null, null, 10L))
    val ignoreNullSelection = ColumnSelection.ExcludeColumns(
      Seq(UnqualifiedColumnName("authoredNull"))
    )

    val result = input.select(
      Scd1VersionMap.buildVersionMap(
        schema = userDataSchema,
        ignoreNullSelection = ignoreNullSelection,
        upsertSequence = F.col("seq"),
        sequencingType = LongType,
        resolver = caseSensitiveResolution
      ).as("versionMap")
    )

    checkAnswer(result, Row(Map[String, Any](
      encodedPath("ignoredNull") -> null,
      encodedPath("authoredNull") -> 10L
    )))
  }

  test("buildVersionMap recursively expands structs and treats other complex types as leaves") {
    val userDataSchema = new StructType()
      .add("profile", new StructType()
        .add("city", StringType)
        .add("contact", new StructType().add("zip", StringType)))
      .add("profile.city", StringType)
      .add("tags", ArrayType(StringType))
      .add("properties", MapType(StringType, StringType))
    val inputSchema = StructType(userDataSchema.fields :+ StructField("seq", LongType))
    val input = dataFrameOf(inputSchema)(
      Row(Row(null, Row(null)), null, null, null, 10L)
    )
    val ignoreNullSelection = ColumnSelection.IncludeColumns(Seq(
      UnqualifiedColumnName("profile"),
      UnqualifiedColumnName("tags")
    ))

    val result = input.select(
      Scd1VersionMap.buildVersionMap(
        schema = userDataSchema,
        ignoreNullSelection = ignoreNullSelection,
        upsertSequence = F.col("seq"),
        sequencingType = LongType,
        resolver = caseSensitiveResolution
      ).as("versionMap")
    )

    checkAnswer(result, Row(Map[String, Any](
      encodedPath("profile", "city") -> null,
      encodedPath("profile", "contact", "zip") -> null,
      encodedPath("profile.city") -> 10L,
      encodedPath("tags") -> null,
      encodedPath("properties") -> 10L
    )))
  }

  test("buildVersionMap respects the supplied resolver for ignore-null selection") {
    val userDataSchema = new StructType().add("Value", StringType)
    val inputSchema = StructType(userDataSchema.fields :+ StructField("seq", LongType))
    val input = dataFrameOf(inputSchema)(Row(null, 10L))
    val ignoreNullSelection = ColumnSelection.IncludeColumns(
      Seq(UnqualifiedColumnName("value"))
    )

    val result = input.select(
      Scd1VersionMap.buildVersionMap(
        schema = userDataSchema,
        ignoreNullSelection = ignoreNullSelection,
        upsertSequence = F.col("seq"),
        sequencingType = LongType,
        resolver = caseInsensitiveResolution
      ).as("versionMap")
    )

    checkAnswer(result, Row(Map[String, Any](encodedPath("Value") -> null)))
  }

  test("buildVersionMap produces a typed empty map for an empty schema") {
    val input = dataFrameOf(new StructType().add("seq", LongType))(Row(10L))

    val result = input.select(
      Scd1VersionMap.buildVersionMap(
        schema = new StructType(),
        ignoreNullSelection = ColumnSelection.ExcludeColumns(Seq.empty),
        upsertSequence = F.col("seq"),
        sequencingType = LongType,
        resolver = caseSensitiveResolution
      ).as("versionMap")
    )

    assert(result.schema("versionMap").dataType == Scd1VersionMap.mapType(LongType))
    checkAnswer(result, Row(Map.empty[String, Long]))
  }

  // Version-map keys are persisted in target tables, so their format must not change.
  test("version map keys have a stable persisted serialization") {
    val leafPathsAndExpectedKeys = Seq(
      Seq("simple") -> "`simple`",
      Seq("a.b") -> "`a.b`",
      Seq("a", "b") -> "`a`.`b`",
      Seq("wrapper", "leaf") -> "`wrapper`.`leaf`",
      Seq("wrapper", "a.b") -> "`wrapper`.`a.b`",
      Seq("wrapper", "has space") -> "`wrapper`.`has space`",
      Seq("wrapper", "col-one") -> "`wrapper`.`col-one`",
      Seq("wrapper", "back`tick") -> "`wrapper`.`back``tick`",
      Seq("wrapper", "`already quoted`") -> "`wrapper`.```already quoted```",
      Seq("wrapper", "single'quote") -> "`wrapper`.`single'quote`",
      Seq("wrapper", "double\"quote") -> "`wrapper`.`double\"quote`",
      Seq("wrapper", "back\\slash") -> "`wrapper`.`back\\slash`",
      Seq("wrapper", "") -> "`wrapper`.``",
      Seq("wrapper", "null" + 0.toChar + "byte") -> ("`wrapper`.`null" + 0.toChar + "byte`")
    )
    val wrapperLeafNames = leafPathsAndExpectedKeys.collect {
      case (Seq("wrapper", name), _) => name
    }
    val userDataSchema = new StructType()
      .add("simple", StringType)
      .add("a.b", StringType)
      .add("a", new StructType().add("b", StringType))
      .add("wrapper", StructType(wrapperLeafNames.map(StructField(_, StringType))))
    val inputSchema = StructType(userDataSchema.fields :+ StructField("seq", LongType))
    val input = dataFrameOf(inputSchema)(
      Row(null, null, Row(null), Row.fromSeq(Seq.fill[Any](wrapperLeafNames.size)(null)), 10L))

    // Selecting every leaf makes buildVersionMap resolve each leaf's column through its key.
    val result = input.select(
      Scd1VersionMap.buildVersionMap(
        schema = userDataSchema,
        ignoreNullSelection = ColumnSelection.ExcludeColumns(Seq.empty),
        upsertSequence = F.col("seq"),
        sequencingType = LongType,
        resolver = caseSensitiveResolution
      )
    )

    checkAnswer(result, Row(leafPathsAndExpectedKeys.map { case (_, key) => key -> null }.toMap))
  }

  test("buildVersionMap keys round-trip and resolve special-character leaf names") {
    val specialNames =
      Seq("a.b", "has space", "back`tick", "quote\"", "back\\slash", "null" + 0.toChar + "byte")
    val userDataSchema = new StructType()
      .add("wrapper", StructType(specialNames.map(StructField(_, StringType))))
    val inputSchema = StructType(userDataSchema.fields :+ StructField("seq", LongType))
    val input = dataFrameOf(inputSchema)(Row(Row(specialNames: _*), 10L))
    val ignoreNullSelection =
      ColumnSelection.IncludeColumns(Seq(UnqualifiedColumnName("wrapper")))

    val versionMap = input.select(
      Scd1VersionMap.buildVersionMap(
        schema = userDataSchema,
        ignoreNullSelection = ignoreNullSelection,
        upsertSequence = F.col("seq"),
        sequencingType = LongType,
        resolver = caseSensitiveResolution
      )
    ).head().getMap[String, Long](0)

    assert(versionMap.keys.map(CatalystSqlParser.parseMultipartIdentifier).toSet ==
      specialNames.map(name => Seq("wrapper", name)).toSet)
    assert(versionMap.values.toSet == Set(10L))
  }

  test("buildVersionMap supports struct-valued sequencing") {
    val sequencingType = new StructType()
      .add("timestamp", LongType)
      .add("tieBreaker", IntegerType)
    val userDataSchema = new StructType()
      .add("selectedNull", StringType)
      .add("selectedValue", StringType)
      .add("unselectedNull", StringType)
    val inputSchema = StructType(userDataSchema.fields :+ StructField("seq", sequencingType))
    val input = dataFrameOf(inputSchema)(Row(null, "selected", null, Row(10L, 1)))
    val ignoreNullSelection = ColumnSelection.IncludeColumns(Seq(
      UnqualifiedColumnName("selectedNull"),
      UnqualifiedColumnName("selectedValue")
    ))

    val result = input.select(
      Scd1VersionMap.buildVersionMap(
        schema = userDataSchema,
        ignoreNullSelection = ignoreNullSelection,
        upsertSequence = F.col("seq"),
        sequencingType = sequencingType,
        resolver = caseSensitiveResolution
      ).as("versionMap")
    )

    assert(result.schema("versionMap").dataType == Scd1VersionMap.mapType(sequencingType))
    checkAnswer(result, Row(Map[String, Any](
      encodedPath("selectedNull") -> null,
      encodedPath("selectedValue") -> Row(10L, 1),
      encodedPath("unselectedNull") -> Row(10L, 1)
    )))
  }
}
