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

  testConstantColumnVector("not supported: fill map", 10,
    MapType(IntegerType, BooleanType)) { vector =>
    val message = intercept[RuntimeException] {
      ColumnVectorUtils.populate(vector, InternalRow("fakeMap"), 0)
    }.getMessage
    assert(message == "DataType MAP<INT, BOOLEAN> is not supported in column vectorized reader.")
  }

  testConstantColumnVector("not supported: fill struct", 10,
    new StructType()
      .add(StructField("name", StringType))
      .add(StructField("age", IntegerType))) { vector =>
    val message = intercept[RuntimeException] {
      ColumnVectorUtils.populate(vector, InternalRow("fakeStruct"), 0)
    }.getMessage
    assert(message ==
      "DataType STRUCT<name: STRING, age: INT> is not supported in column vectorized reader.")
  }

  testConstantColumnVector("not supported: fill array", 10,
    ArrayType(IntegerType)) { vector =>
    val message = intercept[RuntimeException] {
      ColumnVectorUtils.populate(vector, InternalRow("fakeArray"), 0)
    }.getMessage
    assert(message == "DataType ARRAY<INT> is not supported in column vectorized reader.")
  }
}
