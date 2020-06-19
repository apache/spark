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

package org.apache.spark.sql.util

import java.time.ZoneId

import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.LA
import org.apache.spark.sql.types._

class ArrowUtilsSuite extends SparkFunSuite {

  def roundtrip(dt: DataType): Unit = {
    dt match {
      case schema: StructType =>
        assert(ArrowUtils.fromArrowSchema(ArrowUtils.toArrowSchema(schema, null)) === schema)
      case _ =>
        roundtrip(new StructType().add("value", dt))
    }
  }

  test("simple") {
    roundtrip(BooleanType)
    roundtrip(ByteType)
    roundtrip(ShortType)
    roundtrip(IntegerType)
    roundtrip(LongType)
    roundtrip(FloatType)
    roundtrip(DoubleType)
    roundtrip(StringType)
    roundtrip(BinaryType)
    roundtrip(DecimalType.SYSTEM_DEFAULT)
    roundtrip(DateType)
    val tsExMsg = intercept[UnsupportedOperationException] {
      roundtrip(TimestampType)
    }
    assert(tsExMsg.getMessage.contains("timeZoneId"))
  }

  test("timestamp") {

    def roundtripWithTz(timeZoneId: String): Unit = {
      val schema = new StructType().add("value", TimestampType)
      val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
      val fieldType = arrowSchema.findField("value").getType.asInstanceOf[ArrowType.Timestamp]
      assert(fieldType.getTimezone() === timeZoneId)
      assert(ArrowUtils.fromArrowSchema(arrowSchema) === schema)
    }

    roundtripWithTz(ZoneId.systemDefault().getId)
    roundtripWithTz("Asia/Tokyo")
    roundtripWithTz("UTC")
    roundtripWithTz(LA.getId)
  }

  test("array") {
    roundtrip(ArrayType(IntegerType, containsNull = true))
    roundtrip(ArrayType(IntegerType, containsNull = false))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = true))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = false))
    roundtrip(ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = false))
  }

  test("struct") {
    roundtrip(new StructType())
    roundtrip(new StructType().add("i", IntegerType))
    roundtrip(new StructType().add("arr", ArrayType(IntegerType)))
    roundtrip(new StructType().add("i", IntegerType).add("arr", ArrayType(IntegerType)))
    roundtrip(new StructType().add(
      "struct",
      new StructType().add("i", IntegerType).add("arr", ArrayType(IntegerType))))
  }
}
