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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.hadoop.conf.Configuration
import org.apache.orc.TypeDescription

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.types._

class OrcUtilsSuite extends SparkFunSuite {

  private def requestedColumnIds(
      orcSchema: TypeDescription,
      dataSchema: StructType): Option[(Array[Int], Boolean)] = {
    OrcUtils.requestedColumnIds(
      isCaseSensitive = false, dataSchema, dataSchema, orcSchema, new Configuration())
  }

  test("requestedColumnIds does not convert file columns read without a timestamp") {
    // A union has no Spark SQL type, so converting this file column would fail to parse.
    val orcSchema = TypeDescription.fromString("struct<a:int,u:uniontype<int,string>>")
    val dataSchema = new StructType().add("a", IntegerType).add("u", StringType)
    val result = requestedColumnIds(orcSchema, dataSchema)
    assert(result.map { case (ids, canPruneCols) => (ids.toSeq, canPruneCols) } ===
      Some((Seq(0, 1), true)))
  }

  test("requestedColumnIds rejects a timestamp mismatch in a later or nested column") {
    def schema(fields: (String, DataType)*): StructType =
      StructType(fields.map { case (name, dt) => StructField(name, dt) })
    Seq(
      // The mismatched column follows non-timestamp columns, so the pairing must stay aligned.
      schema("a" -> IntegerType, "b" -> StringType, "ts" -> TimestampType) ->
        schema("a" -> IntegerType, "b" -> StringType, "ts" -> TimestampNTZType),
      schema("m" -> MapType(StringType, TimestampType)) ->
        schema("m" -> MapType(StringType, TimestampNTZType)),
      schema("s" -> schema("i" -> IntegerType, "ts" -> TimestampType)) ->
        schema("s" -> schema("i" -> IntegerType, "ts" -> TimestampNTZType))
    ).foreach { case (fileSchema, readSchema) =>
      withClue(s"$fileSchema -> $readSchema") {
        checkError(
          exception = intercept[SparkUnsupportedOperationException] {
            requestedColumnIds(OrcUtils.orcTypeDescription(fileSchema), readSchema)
          },
          condition = "UNSUPPORTED_FEATURE.ORC_TYPE_CAST",
          parameters = Map("orcType" -> "\"TIMESTAMP\"", "toType" -> "\"TIMESTAMP_NTZ\""))
      }
    }
  }

  test("requestedColumnIds rejects a timestamp mismatch next to an unconverted column") {
    val orcSchema = TypeDescription.fromString("struct<u:uniontype<int,string>,ts:timestamp>")
    val dataSchema = new StructType().add("u", StringType).add("ts", TimestampNTZType)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        requestedColumnIds(orcSchema, dataSchema)
      },
      condition = "UNSUPPORTED_FEATURE.ORC_TYPE_CAST",
      parameters = Map("orcType" -> "\"TIMESTAMP\"", "toType" -> "\"TIMESTAMP_NTZ\""))
  }

  test("requestedColumnIds accepts nanos timestamps of the same kind at another precision") {
    val fileSchema = new StructType()
      .add("ntz", TimestampNTZNanosType(9))
      .add("ltz", ArrayType(TimestampLTZNanosType(9)))
    val readSchema = new StructType()
      .add("ntz", TimestampNTZNanosType(7))
      .add("ltz", ArrayType(TimestampLTZNanosType(8)))
    val result = requestedColumnIds(OrcUtils.orcTypeDescription(fileSchema), readSchema)
    assert(result.map { case (ids, canPruneCols) => (ids.toSeq, canPruneCols) } ===
      Some((Seq(0, 1), true)))
  }
}
