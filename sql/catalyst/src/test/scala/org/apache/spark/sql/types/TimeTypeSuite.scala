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

package org.apache.spark.sql.types

import org.apache.spark.SparkFunSuite

class TimeTypeSuite extends SparkFunSuite {

  test("TimeType - basic properties") {
    assert(TimeType.typeName === "time")
    assert(TimeType.defaultSize === 8) // Long size
    assert(TimeType.simpleString === "time")
    assert(TimeType.sql === "TIME")
  }

  test("TimeType - JSON serialization") {
    val json = TimeType.json
    assert(json === "\"time\"")

    val prettyJson = TimeType.prettyJson
    assert(prettyJson === "\"time\"")
  }

  test("TimeType - JSON deserialization") {
    val dataType = DataType.fromJson("\"time\"")
    assert(dataType === TimeType)
  }

  test("TimeType - equality") {
    assert(TimeType === TimeType)
    assert(TimeType == TimeType)
    assert(TimeType.sameType(TimeType))
  }

  test("TimeType - inequality with other types") {
    assert(TimeType !== TimestampType)
    assert(TimeType !== DateType)
    assert(TimeType !== LongType)
    assert(TimeType !== StringType)
    assert(!TimeType.sameType(TimestampType))
  }

  test("TimeType - catalog string") {
    assert(TimeType.catalogString === "time")
  }

  test("TimeType - asNullable") {
    assert(TimeType.asNullable === TimeType)
  }

  test("TimeType - acceptsType") {
    assert(TimeType.acceptsType(TimeType))
    assert(!TimeType.acceptsType(TimestampType))
    assert(!TimeType.acceptsType(DateType))
  }

  test("TimeType - in StructType") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("event_time", TimeType),
      StructField("name", StringType)
    ))

    assert(schema.fields(1).dataType === TimeType)
    assert(schema.fields(1).name === "event_time")
  }

  test("TimeType - JSON roundtrip in StructType") {
    val schema = StructType(Seq(
      StructField("event_time", TimeType, nullable = true)
    ))

    val json = schema.json
    val parsedSchema = DataType.fromJson(json).asInstanceOf[StructType]

    assert(parsedSchema.fields(0).dataType === TimeType)
    assert(parsedSchema.fields(0).name === "event_time")
    assert(parsedSchema.fields(0).nullable === true)
  }

  test("TimeType - in ArrayType") {
    val arrayType = ArrayType(TimeType, containsNull = true)
    assert(arrayType.elementType === TimeType)

    val json = arrayType.json
    val parsed = DataType.fromJson(json).asInstanceOf[ArrayType]
    assert(parsed.elementType === TimeType)
  }

  test("TimeType - in MapType") {
    val mapType = MapType(StringType, TimeType, valueContainsNull = true)
    assert(mapType.valueType === TimeType)

    val json = mapType.json
    val parsed = DataType.fromJson(json).asInstanceOf[MapType]
    assert(parsed.valueType === TimeType)
  }

  test("TimeType - DDL parsing") {
    val dataType = DataType.fromDDL("time")
    assert(dataType === TimeType)
  }

  test("TimeType - complex schema DDL") {
    val ddl = "id INT, event_time TIME, name STRING"
    val schema = DataType.fromDDL(ddl).asInstanceOf[StructType]

    assert(schema.fields.length === 3)
    assert(schema.fields(1).dataType === TimeType)
    assert(schema.fields(1).name === "event_time")
  }
}
