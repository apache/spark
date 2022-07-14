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

package org.apache.spark.sql.execution

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class RowToColumnConverterSuite extends SparkFunSuite {
  def convertRows(rows: Seq[InternalRow], schema: StructType): Seq[WritableColumnVector] = {
    val converter = new RowToColumnConverter(schema)
    val vectors =
      schema.map(f => new OnHeapColumnVector(5, f.dataType)).toArray[WritableColumnVector]
    for (row <- rows) {
      converter.convert(row, vectors)
    }
    vectors
  }

  test("integer column") {
    val schema = StructType(Seq(StructField("i", IntegerType)))
    val rows = (0 until 100).map(i => InternalRow(i))
    val vectors = convertRows(rows, schema)
    rows.zipWithIndex.map { case (row, i) =>
      assert(vectors.head.getInt(i) === row.getInt(0))
    }
  }

  test("array column") {
    val schema = StructType(Seq(StructField("a", ArrayType(IntegerType))))
    val rows = (0 until 100).map { i =>
      InternalRow(new GenericArrayData(0 until i))
    }
    val vectors = convertRows(rows, schema)
    rows.zipWithIndex.map { case (row, i) =>
      assert(vectors.head.getArray(i).array().array === row.getArray(0).array)
    }
  }

  test("non-nullable array column with null elements") {
    val arrayType = ArrayType(IntegerType, containsNull = true)
    val schema = StructType(Seq(StructField("a", arrayType, nullable = false)))
    val rows = (0 until 100).map { i =>
      InternalRow(new GenericArrayData((0 until i).map { j =>
        if (j % 3 == 0) {
          null
        } else {
          j
        }
      }))
    }
    val vectors = convertRows(rows, schema)
    rows.zipWithIndex.map { case (row, i) =>
      assert(vectors.head.getArray(i).array().array === row.getArray(0).array)
    }
  }

  test("nested array column") {
    val arrayType = ArrayType(ArrayType(IntegerType))
    val schema = StructType(Seq(StructField("a", arrayType)))
    val rows = (0 until 100).map { i =>
      InternalRow(new GenericArrayData((0 until i).map(j => new GenericArrayData(0 until j))))
    }
    val vectors = convertRows(rows, schema)
    rows.zipWithIndex.map { case (row, i) =>
      val result = vectors.head.getArray(i).array().array
        .map(_.asInstanceOf[ArrayData].array)
      val expected = row.getArray(0).array
        .map(_.asInstanceOf[ArrayData].array)
      assert(result === expected)
    }
  }

  test("map column") {
    val mapType = MapType(IntegerType, StringType)
    val schema = StructType(Seq(StructField("m", mapType)))
    val rows = (0 until 100).map { i =>
      InternalRow(new ArrayBasedMapData(
        new GenericArrayData(0 until i),
        new GenericArrayData((0 until i).map(j => UTF8String.fromString(s"str$j")))))
    }
    val vectors = convertRows(rows, schema)
    rows.zipWithIndex.map { case (row, i) =>
      val result = vectors.head.getMap(i)
      val expected = row.getMap(0)
      assert(result.keyArray().array().array === expected.keyArray().array)
      assert(result.valueArray().array().array === expected.valueArray().array)
    }
  }

  test("non-nullable map column with null values") {
    val mapType = MapType(IntegerType, StringType, valueContainsNull = true)
    val schema = StructType(Seq(StructField("m", mapType, nullable = false)))
    val rows = (0 until 100).map { i =>
      InternalRow(new ArrayBasedMapData(
        new GenericArrayData(0 until i),
        new GenericArrayData((0 until i).map { j =>
          if (j % 3 == 0) {
            null
          } else {
            UTF8String.fromString(s"str$j")
          }
        })))
    }
    val vectors = convertRows(rows, schema)
    rows.zipWithIndex.map { case (row, i) =>
      val result = vectors.head.getMap(i)
      val expected = row.getMap(0)
      assert(result.keyArray().array().array === expected.keyArray().array)
      assert(result.valueArray().array().array === expected.valueArray().array)
    }
  }

  test("multiple columns") {
    val schema = StructType(
      Seq(StructField("s", ShortType), StructField("i", IntegerType), StructField("l", LongType)))
    val rows = (0 until 100).map { i =>
      InternalRow((3 * i).toShort, 3 * i + 1, (3 * i + 2).toLong)
    }
    val vectors = convertRows(rows, schema)
    rows.zipWithIndex.map { case (row, i) =>
      assert(vectors(0).getShort(i) === row.getShort(0))
      assert(vectors(1).getInt(i) === row.getInt(1))
      assert(vectors(2).getLong(i) === row.getLong(2))
    }
  }
}
