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

package org.apache.spark.sql.catalyst.util

import scala.util.Random

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.encoders.{ExamplePointUDT, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{SafeProjection, UnsafeProjection}
import org.apache.spark.sql.types._

class ArrayDataIndexedSeqSuite extends SparkFunSuite {
  private def compArray(arrayData: ArrayData, elementDt: DataType, array: Array[Any]): Unit = {
    assert(arrayData.numElements() == array.length)
    array.zipWithIndex.map { case (e, i) =>
      if (e != null) {
        elementDt match {
          // For NaN, etc.
          case FloatType | DoubleType => assert(arrayData.get(i, elementDt).equals(e))
          case _ => assert(arrayData.get(i, elementDt) === e)
        }
      } else {
        assert(arrayData.isNullAt(i))
      }
    }

    val seq = arrayData.toSeq[Any](elementDt)
    array.zipWithIndex.map { case (e, i) =>
      if (e != null) {
        elementDt match {
          // For Nan, etc.
          case FloatType | DoubleType => assert(seq(i) == e)
          case _ => assert(seq(i) === e)
        }
      } else {
        assert(seq(i) == null)
      }
    }

    Seq(-1, seq.length).foreach { index =>
      checkError(
        exception = intercept[SparkException] {
          seq(index)
        },
        condition = "INTERNAL_ERROR",
        parameters = Map(
          "message" -> s"Index $index must be between 0 and the length of the ArrayData."))
    }
  }

  private def testArrayData(): Unit = {
    val elementTypes = Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType,
      DoubleType, DecimalType.USER_DEFAULT, StringType, BinaryType, DateType, TimestampType,
      CalendarIntervalType, new ExamplePointUDT())
    val arrayTypes = elementTypes.flatMap { elementType =>
      Seq(ArrayType(elementType, containsNull = false), ArrayType(elementType, containsNull = true))
    }
    val random = new Random(100)
    arrayTypes.foreach { dt =>
      val schema = StructType(StructField("col_1", dt, nullable = false) :: Nil)
      val row = RandomDataGenerator.randomRow(random, schema)
      val toRow = ExpressionEncoder(schema).createSerializer()
      val internalRow = toRow(row)

      val unsafeRowConverter = UnsafeProjection.create(schema)
      val safeRowConverter = SafeProjection.create(schema)

      val unsafeRow = unsafeRowConverter(internalRow)
      val safeRow = safeRowConverter(unsafeRow)

      val genericArrayData = safeRow.getArray(0).asInstanceOf[GenericArrayData]
      val unsafeArrayData = unsafeRow.getArray(0)

      val elementType = dt.elementType
      test("ArrayDataIndexedSeq - UnsafeArrayData - " + dt.toString) {
        compArray(unsafeArrayData, elementType, unsafeArrayData.toArray[Any](elementType))
      }

      test("ArrayDataIndexedSeq - GenericArrayData - " + dt.toString) {
        compArray(genericArrayData, elementType, genericArrayData.toArray[Any](elementType))
      }
    }
  }

  testArrayData()
}
