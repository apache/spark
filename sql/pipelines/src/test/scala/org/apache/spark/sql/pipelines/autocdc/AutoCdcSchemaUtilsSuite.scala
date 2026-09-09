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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class AutoCdcSchemaUtilsSuite extends SparkFunSuite {

  test("extractLeafPaths returns single-element paths for flat columns") {
    val schema = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
      .add("c", DoubleType)

    assert(AutoCdcSchemaUtils.extractLeafPaths(schema) ===
      Seq(Seq("a"), Seq("b"), Seq("c")))
  }

  test("extractLeafPaths returns leaves rather than intermediate structs") {
    val schema = new StructType()
      .add("x", IntegerType)
      .add("address", new StructType()
        .add("city", StringType)
        .add("zip", IntegerType))

    assert(AutoCdcSchemaUtils.extractLeafPaths(schema) ===
      Seq(Seq("x"), Seq("address", "city"), Seq("address", "zip")))
  }

  test("extractLeafPaths returns full paths for deeply nested structs") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("mid", new StructType()
          .add("leaf", StringType)))

    assert(AutoCdcSchemaUtils.extractLeafPaths(schema) ===
      Seq(Seq("top", "mid", "leaf")))
  }

  test("extractLeafPaths treats arrays and maps as opaque leaves") {
    val schema = new StructType()
      .add("tags", ArrayType(StringType))
      .add("props", MapType(StringType, IntegerType))
      .add("plain", IntegerType)

    assert(AutoCdcSchemaUtils.extractLeafPaths(schema) ===
      Seq(Seq("tags"), Seq("props"), Seq("plain")))
  }

  test("extractLeafPaths returns an empty sequence for an empty schema") {
    assert(AutoCdcSchemaUtils.extractLeafPaths(new StructType()) === Seq.empty)
  }
}
