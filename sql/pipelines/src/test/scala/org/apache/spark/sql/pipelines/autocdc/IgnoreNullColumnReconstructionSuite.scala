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

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.{functions => F, Column, QueryTest, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class IgnoreNullColumnReconstructionSuite extends QueryTest with SharedSparkSession {

  /** Selects `field` from `source`, reconstructed from `leafValues`. */
  private def reconstruct(
      field: StructField,
      leafValues: Seq[(Seq[String], Column)],
      source: DataFrame = spark.range(1).toDF()): DataFrame =
    source.select(
      IgnoreNullColumnReconstruction.reconstructColumn(
        path = Seq(field.name),
        field = field,
        leafValues = leafValues))

  test("rebuilds a struct recursively from the values of its leaves") {
    // Name parts are taken as is, so `zip.code` names a single field.
    val locationType = new StructType().add("city", StringType).add("zip.code", StringType)
    // An array is a leaf, even if its elements are structs.
    val tagsType = ArrayType(new StructType().add("label", StringType))
    val profile = StructField(
      "profile",
      new StructType().add("location", locationType).add("name", StringType).add("tags", tagsType))

    checkAnswer(
      reconstruct(profile, Seq(
        Seq("profile", "location", "city") -> F.lit("SF"),
        Seq("profile", "location", "zip.code") -> F.lit("94107"),
        Seq("profile", "name") -> F.lit("Ada"),
        Seq("profile", "tags") -> F.array(F.struct(F.lit("home"))))),
      Row(Row(Row("SF", "94107"), "Ada", Seq(Row("home")))))
  }

  test("rebuilds a struct whose leaves are all null as a struct of nulls") {
    // Every leaf is nullable, so even the non-nullable `location` can be a struct of nulls.
    val locationType = new StructType().add("city", StringType).add("zip", StringType)
    val profile = StructField(
      "profile",
      new StructType().add("location", locationType, nullable = false).add("name", StringType))
    def leaves(city: Column, zip: Column, name: Column): Seq[(Seq[String], Column)] = Seq(
      Seq("profile", "location", "city") -> city,
      Seq("profile", "location", "zip") -> zip,
      Seq("profile", "name") -> name)

    checkAnswer(
      reconstruct(profile, leaves(F.lit(null), F.lit(null), F.lit(null))),
      Row(Row(Row(null, null), null)))
    checkAnswer(
      reconstruct(profile, leaves(F.lit(null), F.lit(null), F.lit("Ada"))),
      Row(Row(Row(null, null), "Ada")))
  }

  test("nulls a struct whose leaves are all null if its schema forbids a struct of nulls") {
    val locationType = StructType(Seq(StructField("city", StringType, nullable = false)))
    // `profile` cannot be a struct of nulls, since `location` and `city` are non-nullable, but
    // `profile` itself is nullable, so it is null instead.
    val profile = StructField(
      "profile", StructType(Seq(StructField("location", locationType, nullable = false))))
    // `address` can be a struct of nulls around a null `home`.
    val address = StructField(
      "address", new StructType().add("home", locationType).add("note", StringType))
    def addressLeaves(city: Column, note: Column): Seq[(Seq[String], Column)] = Seq(
      Seq("address", "home", "city") -> city,
      Seq("address", "note") -> note)

    checkAnswer(
      reconstruct(profile, Seq(Seq("profile", "location", "city") -> F.lit(null))),
      Row(null))
    checkAnswer(
      reconstruct(profile, Seq(Seq("profile", "location", "city") -> F.lit("SF"))),
      Row(Row(Row("SF"))))
    checkAnswer(
      reconstruct(address, addressLeaves(F.lit(null), F.lit(null))),
      Row(Row(null, null)))
    checkAnswer(
      reconstruct(address, addressLeaves(F.lit(null), F.lit("note"))),
      Row(Row(null, "note")))
    checkAnswer(
      reconstruct(address, addressLeaves(F.lit("LA"), F.lit(null))),
      Row(Row(Row("LA"), null)))
  }

  test("keeps the declared type, nullability, and metadata of every field") {
    def commentMetadata(comment: String): Metadata =
      new MetadataBuilder().putString("comment", comment).build()

    val locationType = StructType(Seq(
      StructField("city", StringType, nullable = false, commentMetadata("city")),
      StructField("zip", LongType, nullable = true, commentMetadata("zip"))))
    val profileType = StructType(Seq(
      StructField("location", locationType, nullable = true, commentMetadata("location")),
      StructField("note", StringType, nullable = true, commentMetadata("note"))))
    val profile = StructField("profile", profileType, nullable = true, commentMetadata("profile"))
    val source = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("SF"))), new StructType().add("city", StringType))

    // The nullability of every value, and the type of `zip`, differ from those declared.
    val result = reconstruct(
      profile,
      Seq(
        Seq("profile", "location", "city") -> F.col("city"),
        Seq("profile", "location", "zip") -> F.lit(94107),
        Seq("profile", "note") -> F.lit("note")),
      source)

    assert(result.schema == StructType(Seq(profile)))
    checkAnswer(result, Row(Row(Row("SF", 94107L), "note")))
  }

  test("raises NOT_NULL_ASSERT_VIOLATION if a non-nullable field must be reconstructed as null") {
    def checkNotNullAssertViolation(result: DataFrame, walkedTypePath: Seq[String]): Unit =
      checkError(
        exception = intercept[SparkRuntimeException](result.collect()),
        condition = "NOT_NULL_ASSERT_VIOLATION",
        parameters = Map("walkedTypePath" -> walkedTypePath.mkString("\n", "\n", "\n")))

    checkNotNullAssertViolation(
      reconstruct(
        StructField("name", StringType, nullable = false),
        Seq(Seq("name") -> F.lit(null))),
      walkedTypePath = Seq("name"))
    // A null struct cannot replace a struct whose leaves are not all null.
    val profileType = new StructType()
      .add("city", StringType, nullable = false)
      .add("note", StringType)
    checkNotNullAssertViolation(
      reconstruct(
        StructField("profile", profileType),
        Seq(Seq("profile", "city") -> F.lit(null), Seq("profile", "note") -> F.lit("note"))),
      walkedTypePath = Seq("profile", "city"))
  }

  test("rebuilds a struct without leaves as an empty struct") {
    val profile = StructField(
      "profile", new StructType().add("empty", new StructType()).add("name", StringType))

    val result = reconstruct(profile, Seq(Seq("profile", "name") -> F.lit("Ada")))

    assert(result.schema == StructType(Seq(profile)))
    checkAnswer(result, Row(Row(Row(), "Ada")))
    checkAnswer(reconstruct(StructField("empty", new StructType()), Seq.empty), Row(Row()))
  }

  test("fails with an internal error if a leaf has no value") {
    val profile = StructField(
      "profile", new StructType().add("city", StringType).add("zip", StringType))

    checkError(
      exception = intercept[SparkException] {
        reconstruct(profile, Seq(Seq("profile", "city") -> F.lit("SF")))
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "Cannot reconstruct `profile`.`zip` because it has no value."))
  }
}
