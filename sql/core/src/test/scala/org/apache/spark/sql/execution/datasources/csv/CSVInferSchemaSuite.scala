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

package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class InferSchemaSuite extends SparkFunSuite {

  test("String fields types are inferred correctly from null types") {
    assert(CSVInferSchema.inferField(NullType, "") == NullType)
    assert(CSVInferSchema.inferField(NullType, null) == NullType)
    assert(CSVInferSchema.inferField(NullType, "100000000000") == LongType)
    assert(CSVInferSchema.inferField(NullType, "60") == IntegerType)
    assert(CSVInferSchema.inferField(NullType, "3.5") == DoubleType)
    assert(CSVInferSchema.inferField(NullType, "test") == StringType)
    assert(CSVInferSchema.inferField(NullType, "2015-08-20 15:57:00") == TimestampType)
    assert(CSVInferSchema.inferField(NullType, "True") == BooleanType)
    assert(CSVInferSchema.inferField(NullType, "FAlSE") == BooleanType)
  }

  test("String fields types are inferred correctly from other types") {
    assert(CSVInferSchema.inferField(LongType, "1.0") == DoubleType)
    assert(CSVInferSchema.inferField(LongType, "test") == StringType)
    assert(CSVInferSchema.inferField(IntegerType, "1.0") == DoubleType)
    assert(CSVInferSchema.inferField(DoubleType, null) == DoubleType)
    assert(CSVInferSchema.inferField(DoubleType, "test") == StringType)
    assert(CSVInferSchema.inferField(LongType, "2015-08-20 14:57:00") == TimestampType)
    assert(CSVInferSchema.inferField(DoubleType, "2015-08-20 15:57:00") == TimestampType)
    assert(CSVInferSchema.inferField(LongType, "True") == BooleanType)
    assert(CSVInferSchema.inferField(IntegerType, "FALSE") == BooleanType)
    assert(CSVInferSchema.inferField(TimestampType, "FALSE") == BooleanType)
  }

  test("Timestamp field types are inferred correctly from other types") {
    assert(CSVInferSchema.inferField(IntegerType, "2015-08-20 14") == StringType)
    assert(CSVInferSchema.inferField(DoubleType, "2015-08-20 14:10") == StringType)
    assert(CSVInferSchema.inferField(LongType, "2015-08 14:49:00") == StringType)
  }

  test("Boolean fields types are inferred correctly from other types") {
    assert(CSVInferSchema.inferField(LongType, "Fale") == StringType)
    assert(CSVInferSchema.inferField(DoubleType, "TRUEe") == StringType)
  }

  test("Type arrays are merged to highest common type") {
    assert(
      CSVInferSchema.mergeRowTypes(Array(StringType),
        Array(DoubleType)).deep == Array(StringType).deep)
    assert(
      CSVInferSchema.mergeRowTypes(Array(IntegerType),
        Array(LongType)).deep == Array(LongType).deep)
    assert(
      CSVInferSchema.mergeRowTypes(Array(DoubleType),
        Array(LongType)).deep == Array(DoubleType).deep)
  }

  test("Null fields are handled properly when a nullValue is specified") {
    assert(CSVInferSchema.inferField(NullType, "null", "null") == NullType)
    assert(CSVInferSchema.inferField(StringType, "null", "null") == StringType)
    assert(CSVInferSchema.inferField(LongType, "null", "null") == LongType)
    assert(CSVInferSchema.inferField(IntegerType, "\\N", "\\N") == IntegerType)
    assert(CSVInferSchema.inferField(DoubleType, "\\N", "\\N") == DoubleType)
    assert(CSVInferSchema.inferField(TimestampType, "\\N", "\\N") == TimestampType)
    assert(CSVInferSchema.inferField(BooleanType, "\\N", "\\N") == BooleanType)
  }

  test("Merging Nulltypes should yeild Nulltype.") {
    val mergedNullTypes = CSVInferSchema.mergeRowTypes(Array(NullType), Array(NullType))
    assert(mergedNullTypes.deep == Array(NullType).deep)
  }
}
