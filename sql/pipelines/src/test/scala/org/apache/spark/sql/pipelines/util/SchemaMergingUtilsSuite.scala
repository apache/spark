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

  test("SPARK-59268: mergeSchemas drops desired metadata on shared fields") {
    // StructType.merge keeps the left-side (current) metadata for shared
    // fields, so comment or default-value changes in the desired schema
    // are silently lost.

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
}
