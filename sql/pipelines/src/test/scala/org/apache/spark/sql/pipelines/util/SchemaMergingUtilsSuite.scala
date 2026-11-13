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

package org.apache.spark.sql.pipelines.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class SchemaMergingUtilsSuite extends SparkFunSuite {

  test("SPARK-59268: mergeSchemas keeps the table-side metadata on shared fields") {
    // StructType.merge keeps the left-side metadata for shared fields. Callers that need the
    // data-side metadata to win use mergeSchemasWithDataMetadata instead.

    // Comments
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true, "old comment")
    val desiredSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true, "new comment")

    val merged = SchemaMergingUtils.mergeSchemas(
      currentSchema, desiredSchema, caseSensitive = false)
    assert(merged("name").getComment() === Some("old comment"))

    // Default values
    val currentWithDefault = new StructType()
      .add("id", IntegerType, nullable = false)
      .add(StructField("score", IntegerType, nullable = true)
        .withCurrentDefaultValue("0"))
    val desiredWithDefault = new StructType()
      .add("id", IntegerType, nullable = false)
      .add(StructField("score", IntegerType, nullable = true)
        .withCurrentDefaultValue("42"))

    val mergedDefaults = SchemaMergingUtils.mergeSchemas(
      currentWithDefault, desiredWithDefault, caseSensitive = false)
    assert(
      mergedDefaults("score").getCurrentDefaultValue() === Some("0"))
  }

  test("SPARK-59268: mergeSchemasWithDataMetadata takes data-side metadata on shared fields") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false, "id comment")
      .add("changed", StringType, nullable = true, "old comment")
      .add("removed", StringType, nullable = true, "removed comment")
      .add("added", StringType)
      .add(StructField("score", IntegerType).withCurrentDefaultValue("0"))
      .add("onlyInCurrent", StringType, nullable = true, "kept comment")
    val desiredSchema = new StructType()
      .add("id", IntegerType, nullable = false, "id comment")
      .add("changed", StringType, nullable = true, "new comment")
      .add("removed", StringType)
      .add("added", StringType, nullable = true, "added comment")
      .add(StructField("score", IntegerType).withCurrentDefaultValue("42"))
      .add("onlyInDesired", StringType, nullable = true, "new column comment")

    val merged = SchemaMergingUtils.mergeSchemasWithDataMetadata(
      currentSchema, desiredSchema, caseSensitive = true)
    assert(merged.fieldNames.toSeq ===
      Seq("id", "changed", "removed", "added", "score", "onlyInCurrent", "onlyInDesired"))
    assert(merged("id").getComment() === Some("id comment"))
    assert(merged("changed").getComment() === Some("new comment"))
    assert(merged("removed").getComment() === None)
    assert(merged("added").getComment() === Some("added comment"))
    assert(merged("score").getCurrentDefaultValue() === Some("42"))
    assert(merged("onlyInCurrent").getComment() === Some("kept comment"))
    assert(merged("onlyInDesired").getComment() === Some("new column comment"))
  }

  test("SPARK-59268: mergeSchemasWithDataMetadata takes data-side metadata on nested fields") {
    def schema(comment: String): StructType = {
      val inner = new StructType().add("a", IntegerType, nullable = true, comment)
      new StructType()
        .add("s", inner)
        .add("arr", ArrayType(inner))
        .add("m", MapType(StringType, inner))
    }
    val currentSchema = schema("old")
    val desiredSchema = schema("new")

    val merged = SchemaMergingUtils.mergeSchemasWithDataMetadata(
      currentSchema, desiredSchema, caseSensitive = true)
    assert(merged === desiredSchema)
  }

  test("SPARK-59268: mergeSchemasWithDataMetadata keeps the table-side name under " +
    "case-insensitive merge") {
    val currentSchema = new StructType().add("value", StringType, nullable = true, "old")
    val desiredSchema = new StructType().add("Value", StringType, nullable = true, "new")

    val merged = SchemaMergingUtils.mergeSchemasWithDataMetadata(
      currentSchema, desiredSchema, caseSensitive = false)
    assert(merged === new StructType().add("value", StringType, nullable = true, "new"))
  }
}
