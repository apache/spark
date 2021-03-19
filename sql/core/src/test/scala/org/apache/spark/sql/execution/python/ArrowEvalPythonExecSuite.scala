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

package org.apache.spark.sql.execution.python

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.ExampleBoxUDT
import org.apache.spark.sql.test.ExamplePointUDT
import org.apache.spark.sql.types._

class ArrowEvalPythonExecSuite extends SparkFunSuite {
  import ArrowEvalPythonExec.plainSchema

  test("plainSchema: fixed point data type") {
    Seq(
      BinaryType, BooleanType, ByteType, NullType,
      StringType, VarcharType(10), CharType(10),
      DoubleType, FloatType, ShortType, IntegerType, LongType,
      DataTypes.DateType, DataTypes.TimestampType,
      DataTypes.CalendarIntervalType,
      DataTypes.DayTimeIntervalType,
      DataTypes.YearMonthIntervalType,
      DataTypes.createDecimalType()
    ).foreach { tpe =>
      assert(plainSchema(tpe) === tpe)
    }
  }

  test("plainSchema: UDT") {
    assert (plainSchema(new ExamplePointUDT()) === ArrayType(DoubleType, true))

    val exampleBoxUDT = new ExampleBoxUDT()
    assert (plainSchema(exampleBoxUDT) === exampleBoxUDT.sqlType)
  }

  test("plainSchema: complex data type") {
    val plainStructType = StructType(Array(StructField(name = "id", LongType)))
    assert(plainSchema(plainStructType) === plainStructType)
    val arrStructType = StructType(Array(StructField(name = "id_arr", ArrayType(LongType, false))))
    assert(plainSchema(arrStructType) === arrStructType)
    val udtStructType = StructType(Array(StructField(name = "udt", new ExamplePointUDT())))
    val udtStructType2 = StructType(Array(StructField(name = "udt", ArrayType(DoubleType, true))))
    assert(plainSchema(udtStructType) === udtStructType2)

    val plainArrayType = ArrayType(LongType, false)
    assert(plainSchema(plainArrayType) === plainArrayType)
    val udtArrayType = ArrayType(new ExamplePointUDT(), false)
    assert(plainSchema(udtArrayType) === ArrayType(ArrayType(DoubleType, true), false))

    val plainMapType = MapType(LongType, LongType, true)
    assert(plainSchema(plainMapType) === plainMapType)
    val udtMapType = MapType(LongType, new ExamplePointUDT(), false)
    assert(plainSchema(udtMapType) === MapType(LongType, ArrayType(DoubleType, true), false))
  }
}
