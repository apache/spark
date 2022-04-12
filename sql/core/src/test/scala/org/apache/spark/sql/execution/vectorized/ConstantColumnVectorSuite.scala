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

package org.apache.spark.sql.execution.vectorized

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

class ConstantColumnVectorSuite extends SparkFunSuite {

  private def testVector(name: String, size: Int, dt: DataType)
    (f: ConstantColumnVector => Unit): Unit = {
    test(name) {
      val vector = new ConstantColumnVector(size, dt)
      f(vector)
      vector.close()
    }
  }

  testVector("null", 10, IntegerType) { vector =>
    vector.setNull()
    assert(vector.hasNull)
    assert(vector.numNulls() == 10)
    (0 until 10).foreach { i =>
      assert(vector.isNullAt(i))
    }

    vector.setNotNull()
    assert(!vector.hasNull)
    assert(vector.numNulls() == 0)
    (0 until 10).foreach { i =>
      assert(!vector.isNullAt(i))
    }
  }

  testVector("boolean", 10, BooleanType) { vector =>
    vector.setBoolean(true)
    (0 until 10).foreach { i =>
      assert(vector.getBoolean(i))
    }
  }

  testVector("byte", 10, ByteType) { vector =>
    vector.setByte(3.toByte)
    (0 until 10).foreach { i =>
      assert(vector.getByte(i) == 3.toByte)
    }
  }

  testVector("short", 10, ShortType) { vector =>
    vector.setShort(3.toShort)
    (0 until 10).foreach { i =>
      assert(vector.getShort(i) == 3.toShort)
    }
  }

  testVector("int", 10, IntegerType) { vector =>
    vector.setInt(3)
    (0 until 10).foreach { i =>
      assert(vector.getInt(i) == 3)
    }
  }

  testVector("long", 10, LongType) { vector =>
    vector.setLong(3L)
    (0 until 10).foreach { i =>
      assert(vector.getLong(i) == 3L)
    }
  }

  testVector("float", 10, FloatType) { vector =>
    vector.setFloat(3.toFloat)
    (0 until 10).foreach { i =>
      assert(vector.getFloat(i) == 3.toFloat)
    }
  }

  testVector("double", 10, DoubleType) { vector =>
    vector.setDouble(3.toDouble)
    (0 until 10).foreach { i =>
      assert(vector.getDouble(i) == 3.toDouble)
    }
  }

  testVector("array", 10, ArrayType(IntegerType)) { vector =>
    // create an vector with constant array: [0, 1, 2, 3, 4]
    val arrayVector = new OnHeapColumnVector(5, IntegerType)
    (0 until 5).foreach { i =>
      arrayVector.putInt(i, i)
    }
    val columnarArray = new ColumnarArray(arrayVector, 0, 5)

    vector.setArray(columnarArray)

    (0 until 10).foreach { i =>
      assert(vector.getArray(i) == columnarArray)
      assert(vector.getArray(i).toIntArray === Array(0, 1, 2, 3, 4))
    }
  }

  testVector("map", 10, MapType(IntegerType, BooleanType)) { vector =>
    // create an vector with constant map:
    // [(0, true), (1, false), (2, true), (3, false), (4, true)]
    val keys = new OnHeapColumnVector(5, IntegerType)
    val values = new OnHeapColumnVector(5, BooleanType)

    (0 until 5).foreach { i =>
      keys.putInt(i, i)
      values.putBoolean(i, i % 2 == 0)
    }

    val columnarMap = new ColumnarMap(keys, values, 0, 5)
    vector.setMap(columnarMap)

    (0 until 10).foreach { i =>
      assert(vector.getMap(i) == columnarMap)
      assert(vector.getMap(i).keyArray().toIntArray === Array(0, 1, 2, 3, 4))
      assert(vector.getMap(i).valueArray().toBooleanArray ===
        Array(true, false, true, false, true))
    }
  }

  testVector("decimal", 10, DecimalType(10, 0)) { vector =>
    val decimal = Decimal(100L)
    vector.setDecimal(decimal, 10)
    (0 until 10).foreach { i =>
      assert(vector.getDecimal(i, 10, 0) == decimal)
    }
  }

  testVector("utf8string", 10, StringType) { vector =>
    vector.setUtf8String(UTF8String.fromString("hello"))
    (0 until 10).foreach { i =>
      assert(vector.getUTF8String(i) == UTF8String.fromString("hello"))
    }
  }

  testVector("binary", 10, BinaryType) { vector =>
    vector.setBinary("hello".getBytes("utf8"))
    (0 until 10).foreach { i =>
      assert(vector.getBinary(i) === "hello".getBytes("utf8"))
    }
  }

  testVector("struct", 10,
    new StructType()
      .add(StructField("name", StringType))
      .add(StructField("age", IntegerType))) { vector =>

    val nameVector = new ConstantColumnVector(10, StringType)
    nameVector.setUtf8String(UTF8String.fromString("jack"))
    vector.setChild(0, nameVector)

    val ageVector = new ConstantColumnVector(10, IntegerType)
    ageVector.setInt(27)
    vector.setChild(1, ageVector)


    assert(vector.getChild(0) == nameVector)
    assert(vector.getChild(1) == ageVector)
    (0 until 10).foreach { i =>
      assert(vector.getChild(0).getUTF8String(i) == UTF8String.fromString("jack"))
      assert(vector.getChild(1).getInt(i) == 27)
    }

    // another API
    (0 until 10).foreach { i =>
      assert(vector.getStruct(i).get(0, StringType) == UTF8String.fromString("jack"))
      assert(vector.getStruct(i).get(1, IntegerType) == 27)
    }
  }

  testVector("calendar interval", 10, CalendarIntervalType) { vector =>
    val monthsVector = new ConstantColumnVector(10, IntegerType)
    monthsVector.setInt(3)
    val daysVector = new ConstantColumnVector(10, IntegerType)
    daysVector.setInt(25)
    val microsecondsVector = new ConstantColumnVector(10, LongType)
    microsecondsVector.setLong(12345L)

    vector.setChild(0, monthsVector)
    vector.setChild(1, daysVector)
    vector.setChild(2, microsecondsVector)

    (0 until 10).foreach { i =>
      assert(vector.getChild(0).getInt(i) == 3)
      assert(vector.getChild(1).getInt(i) == 25)
      assert(vector.getChild(2).getLong(i) == 12345L)
    }
  }
}
