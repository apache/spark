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

package org.apache.spark.sql

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, SpecificMutableRow}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class RowSuite extends SparkFunSuite with SharedSQLContext {
  import testImplicits._

  test("create row") {
    val expected = new GenericMutableRow(4)
    expected.setInt(0, 2147483647)
    expected.update(1, UTF8String.fromString("this is a string"))
    expected.setBoolean(2, false)
    expected.setNullAt(3)

    val actual1 = Row(2147483647, "this is a string", false, null)
    assert(expected.numFields === actual1.size)
    assert(expected.getInt(0) === actual1.getInt(0))
    assert(expected.getString(1) === actual1.getString(1))
    assert(expected.getBoolean(2) === actual1.getBoolean(2))
    assert(expected.isNullAt(3) === actual1.isNullAt(3))

    val actual2 = Row.fromSeq(Seq(2147483647, "this is a string", false, null))
    assert(expected.numFields === actual2.size)
    assert(expected.getInt(0) === actual2.getInt(0))
    assert(expected.getString(1) === actual2.getString(1))
    assert(expected.getBoolean(2) === actual2.getBoolean(2))
    assert(expected.isNullAt(3) === actual2.isNullAt(3))
  }

  test("SpecificMutableRow.update with null") {
    val row = new SpecificMutableRow(Seq(IntegerType))
    row(0) = null
    assert(row.isNullAt(0))
  }

  test("serialize w/ kryo") {
    val row = Seq((1, Seq(1), Map(1 -> 1), BigDecimal(1))).toDF().first()
    val serializer = new SparkSqlSerializer(ctx.sparkContext.getConf)
    val instance = serializer.newInstance()
    val ser = instance.serialize(row)
    val de = instance.deserialize(ser).asInstanceOf[Row]
    assert(de === row)
  }

  test("get values by field name on Row created via .toDF") {
    val row = Seq((1, Seq(1))).toDF("a", "b").first()
    assert(row.getAs[Int]("a") === 1)
    assert(row.getAs[Seq[Int]]("b") === Seq(1))

    intercept[IllegalArgumentException]{
      row.getAs[Int]("c")
    }
  }

  test("float NaN == NaN") {
    val r1 = Row(Float.NaN)
    val r2 = Row(Float.NaN)
    assert(r1 === r2)
  }

  test("double NaN == NaN") {
    val r1 = Row(Double.NaN)
    val r2 = Row(Double.NaN)
    assert(r1 === r2)
  }

  test("equals and hashCode") {
    val r1 = Row("Hello")
    val r2 = Row("Hello")
    assert(r1 === r2)
    assert(r1.hashCode() === r2.hashCode())
    val r3 = Row("World")
    assert(r3.hashCode() != r1.hashCode())
  }
}
