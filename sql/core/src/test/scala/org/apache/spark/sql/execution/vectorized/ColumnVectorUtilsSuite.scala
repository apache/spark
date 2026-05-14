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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.UTF8String

class ColumnVectorUtilsSuite extends SparkFunSuite {

  private def testConstantColumnVector(name: String, size: Int, dt: DataType)
    (f: ConstantColumnVector => Unit): Unit = {
    test(name) {
      val vector = new ConstantColumnVector(size, dt)
      f(vector)
      vector.close()
    }
  }

  testConstantColumnVector("fill null", 10, IntegerType) { vector =>

    ColumnVectorUtils.populate(vector, InternalRow(null), 0)

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

  testConstantColumnVector("fill boolean", 10, BooleanType) { vector =>
    ColumnVectorUtils.populate(vector, InternalRow(true), 0)
    (0 until 10).foreach { i =>
      assert(vector.getBoolean(i))
    }
  }

  testConstantColumnVector("fill byte", 10, ByteType) { vector =>
    ColumnVectorUtils.populate(vector, InternalRow(3.toByte), 0)
    (0 until 10).foreach { i =>
      assert(vector.getByte(i) == 3.toByte)
    }
  }

  testConstantColumnVector("fill short", 10, ShortType) { vector =>
    ColumnVectorUtils.populate(vector, InternalRow(3.toShort), 0)
    (0 until 10).foreach { i =>
      assert(vector.getShort(i) == 3.toShort)
    }
  }

  testConstantColumnVector("fill int", 10, IntegerType) { vector =>
    ColumnVectorUtils.populate(vector, InternalRow(3), 0)
    (0 until 10).foreach { i =>
      assert(vector.getInt(i) == 3)
    }
  }

  testConstantColumnVector("fill long", 10, LongType) { vector =>
    ColumnVectorUtils.populate(vector, InternalRow(3L), 0)
    (0 until 10).foreach { i =>
      assert(vector.getLong(i) == 3L)
    }
  }

  testConstantColumnVector("fill float", 10, FloatType) { vector =>
    ColumnVectorUtils.populate(vector, InternalRow(3.toFloat), 0)
    (0 until 10).foreach { i =>
      assert(vector.getFloat(i) == 3.toFloat)
    }
  }

  testConstantColumnVector("fill double", 10, DoubleType) { vector =>
    ColumnVectorUtils.populate(vector, InternalRow(3.toDouble), 0)
    (0 until 10).foreach { i =>
      assert(vector.getDouble(i) == 3.toDouble)
    }
  }

  testConstantColumnVector("fill decimal", 10, DecimalType(10, 0)) { vector =>
    val decimal = Decimal(100L)
    ColumnVectorUtils.populate(vector, InternalRow(decimal), 0)
    (0 until 10).foreach { i =>
      assert(vector.getDecimal(i, 10, 0) == decimal)
    }
  }

  testConstantColumnVector("fill utf8string", 10, StringType) { vector =>
    val string = UTF8String.fromString("hello")
    ColumnVectorUtils.populate(vector, InternalRow(string), 0)
    (0 until 10).foreach { i =>
      assert(vector.getUTF8String(i) == string)
    }
  }

  testConstantColumnVector("fill binary", 10, BinaryType) { vector =>
    val binary = "hello".getBytes("utf8")
    ColumnVectorUtils.populate(vector, InternalRow(binary), 0)
    (0 until 10).foreach { i =>
      assert(vector.getBinary(i) === binary)
    }
  }

  testConstantColumnVector("fill calendar interval", 10,
    CalendarIntervalType) { vector =>
    val interval = new CalendarInterval(3, 5, 1000000)
    ColumnVectorUtils.populate(vector, InternalRow(interval), 0)
    (0 until 10).foreach { i =>
      assert(vector.getInterval(i) === interval)
    }
  }

  testConstantColumnVector("fill array of ints", 10, ArrayType(IntegerType)) { vector =>
    val arr = new GenericArrayData(Array[Any](1, 2, 3, 4, 5))
    ColumnVectorUtils.populate(vector, InternalRow(arr), 0)
    (0 until 10).foreach { i =>
      assert(vector.getArray(i).toIntArray === Array(1, 2, 3, 4, 5))
    }
  }

  testConstantColumnVector("fill array of strings", 10, ArrayType(StringType)) { vector =>
    val arr = new GenericArrayData(Array[Any](
      UTF8String.fromString("a"),
      UTF8String.fromString("bb"),
      UTF8String.fromString("ccc")))
    ColumnVectorUtils.populate(vector, InternalRow(arr), 0)
    (0 until 10).foreach { i =>
      val a = vector.getArray(i)
      assert(a.numElements() == 3)
      assert(a.getUTF8String(0) == UTF8String.fromString("a"))
      assert(a.getUTF8String(1) == UTF8String.fromString("bb"))
      assert(a.getUTF8String(2) == UTF8String.fromString("ccc"))
    }
  }

  testConstantColumnVector("fill map of int -> boolean", 10,
    MapType(IntegerType, BooleanType)) { vector =>
    val keys = new GenericArrayData(Array[Any](1, 2, 3))
    val values = new GenericArrayData(Array[Any](true, false, true))
    val map = new ArrayBasedMapData(keys, values)
    ColumnVectorUtils.populate(vector, InternalRow(map), 0)
    (0 until 10).foreach { i =>
      val m = vector.getMap(i)
      assert(m.numElements() == 3)
      assert(m.keyArray().toIntArray === Array(1, 2, 3))
      assert(m.valueArray().toBooleanArray === Array(true, false, true))
    }
  }

  testConstantColumnVector("fill struct", 10,
    new StructType()
      .add(StructField("name", StringType))
      .add(StructField("age", IntegerType))) { vector =>
    val row = InternalRow(UTF8String.fromString("jack"), 27)
    ColumnVectorUtils.populate(vector, InternalRow(row), 0)
    (0 until 10).foreach { i =>
      assert(vector.getChild(0).getUTF8String(i) == UTF8String.fromString("jack"))
      assert(vector.getChild(1).getInt(i) == 27)
    }
  }

  testConstantColumnVector("fill struct with null field", 10,
    new StructType()
      .add(StructField("name", StringType, nullable = true))
      .add(StructField("age", IntegerType))) { vector =>
    val row = InternalRow(null, 27)
    ColumnVectorUtils.populate(vector, InternalRow(row), 0)
    (0 until 10).foreach { i =>
      assert(vector.getChild(0).isNullAt(i))
      assert(vector.getChild(1).getInt(i) == 27)
    }
  }

  testConstantColumnVector("fill nested struct", 10,
    new StructType()
      .add(StructField("inner",
        new StructType()
          .add(StructField("k", StringType))
          .add(StructField("v", IntegerType))))
      .add(StructField("flag", BooleanType))) { vector =>
    val inner = InternalRow(UTF8String.fromString("a"), 1)
    val outer = InternalRow(inner, true)
    ColumnVectorUtils.populate(vector, InternalRow(outer), 0)
    (0 until 10).foreach { i =>
      val s = vector.getChild(0)
      assert(s.getChild(0).getUTF8String(i) == UTF8String.fromString("a"))
      assert(s.getChild(1).getInt(i) == 1)
      assert(vector.getChild(1).getBoolean(i))
    }
  }

  testConstantColumnVector("fill nested array<struct>", 10,
    ArrayType(new StructType()
      .add(StructField("k", StringType))
      .add(StructField("v", IntegerType)))) { vector =>
    val structs = new GenericArrayData(Array[Any](
      InternalRow(UTF8String.fromString("a"), 1),
      InternalRow(UTF8String.fromString("bb"), 2),
      InternalRow(null, 3)))
    ColumnVectorUtils.populate(vector, InternalRow(structs), 0)
    (0 until 10).foreach { i =>
      val a = vector.getArray(i)
      assert(a.numElements() == 3)
      assert(a.getStruct(0, 2).getUTF8String(0) == UTF8String.fromString("a"))
      assert(a.getStruct(0, 2).getInt(1) == 1)
      assert(a.getStruct(1, 2).getUTF8String(0) == UTF8String.fromString("bb"))
      assert(a.getStruct(1, 2).getInt(1) == 2)
      assert(a.getStruct(2, 2).isNullAt(0))
      assert(a.getStruct(2, 2).getInt(1) == 3)
    }
  }

  testConstantColumnVector("fill null array", 10, ArrayType(IntegerType)) { vector =>
    ColumnVectorUtils.populate(vector, InternalRow(null), 0)
    assert(vector.hasNull)
  }
}
