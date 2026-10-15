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

import org.apache.spark.sql.{functions => F, Column, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class Scd1ReconciledFieldSuite extends QueryTest with SharedSparkSession {

  private def encodedPath(parts: String*): String = Scd1VersionMap.serializeKey(parts)

  private def reconciledColumn(
      name: String,
      leafPaths: Seq[Seq[String]],
      valueAuthoredAtSequence: Column): Scd1ReconciledField =
    Scd1ReconciledField(
      fieldToReconcile = Scd1FieldToReconcile(
        path = Seq(name), leafPaths = leafPaths, isIgnoreNullLeaf = false),
      reconciledValue = F.lit(null),
      valueAuthoredAtSequence = valueAuthoredAtSequence)

  private val profileField = reconciledColumn(
    name = "profile",
    leafPaths = Seq(Seq("profile", "city"), Seq("profile", "zip")),
    valueAuthoredAtSequence = F.lit(5L))

  private val nameField = reconciledColumn(
    name = "name",
    leafPaths = Seq(Seq("name")),
    valueAuthoredAtSequence = F.lit(7L))

  test("versionMapFrom records each field's sequence for every leaf at or beneath it") {
    val result = spark.range(1).select(
      Scd1ReconciledField.versionMapFrom(Seq(profileField, nameField), LongType).as("versionMap"))

    assert(result.schema("versionMap").dataType == Scd1VersionMap.mapType(LongType))
    checkAnswer(result, Row(Map(
      encodedPath("profile", "city") -> 5L,
      encodedPath("profile", "zip") -> 5L,
      encodedPath("name") -> 7L)))
  }

  test("versionMapFrom records the leaves of an unauthored field with null sequences") {
    val unauthoredField = reconciledColumn(
      name = "name",
      leafPaths = Seq(Seq("name")),
      valueAuthoredAtSequence = F.lit(null))

    val result = spark.range(1).select(
      Scd1ReconciledField.versionMapFrom(Seq(profileField, unauthoredField), LongType))

    checkAnswer(result, Row(Map[String, Any](
      encodedPath("profile", "city") -> 5L,
      encodedPath("profile", "zip") -> 5L,
      encodedPath("name") -> null)))
  }

  test("versionMapFrom adds no entries for a field without leaves") {
    val emptyStructField = reconciledColumn(
      name = "empty",
      leafPaths = Seq.empty,
      valueAuthoredAtSequence = F.lit(3L))

    val result = spark.range(1).select(
      Scd1ReconciledField.versionMapFrom(Seq(emptyStructField, nameField), LongType))

    checkAnswer(result, Row(Map(encodedPath("name") -> 7L)))
  }

  test("versionMapFrom builds a typed empty map without leaves under legacy collection types") {
    val sequencingType = new StructType()
      .add("timestamp", LongType)
      .add("tieBreaker", IntegerType)
    val emptyStructField = reconciledColumn(
      name = "empty",
      leafPaths = Seq.empty,
      valueAuthoredAtSequence = F.struct(F.lit(10L).as("timestamp"), F.lit(1).as("tieBreaker")))

    withSQLConf(SQLConf.LEGACY_CREATE_EMPTY_COLLECTION_USING_STRING_TYPE.key -> "true") {
      val result = spark.range(1).select(
        Scd1ReconciledField.versionMapFrom(Seq(emptyStructField), sequencingType).as("versionMap"))

      assert(result.schema("versionMap").dataType == Scd1VersionMap.mapType(sequencingType))
      checkAnswer(result, Row(Map.empty[String, Row]))
    }
  }

  test("versionMapFrom casts sequences to the sequencing type") {
    val sequencingType = new StructType()
      .add("timestamp", LongType)
      .add("tieBreaker", IntegerType)
    val field = reconciledColumn(
      name = "name",
      leafPaths = Seq(Seq("name")),
      valueAuthoredAtSequence = F.struct(F.lit(10).as("timestamp"), F.lit(1).as("tieBreaker")))

    val result = spark.range(1).select(
      Scd1ReconciledField.versionMapFrom(Seq(field), sequencingType).as("versionMap"))

    assert(result.schema("versionMap").dataType == Scd1VersionMap.mapType(sequencingType))
    checkAnswer(result, Row(Map(encodedPath("name") -> Row(10L, 1))))
  }

  test("lastAuthoredIn reads back each field's sequence from the version map it builds") {
    val fields = Seq(profileField, nameField)
    val cdcMetadata = Scd1BatchProcessor.constructCdcMetadataCol(
      deleteSequence = F.lit(null),
      upsertSequence = F.lit(7L),
      versionMap = Scd1ReconciledField.versionMapFrom(fields, LongType),
      sequencingType = LongType)

    val result = spark.range(1).select(fields.map { field =>
      Scd1BatchProcessor.lastAuthoredIn(cdcMetadata, field.fieldToReconcile.leafPaths)
    }: _*)

    checkAnswer(result, Row(5L, 7L))
  }
}
