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

import org.apache.spark.sql.execution.SparkSqlSerializer
import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, SpecificMutableRow}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext.implicits._
import org.apache.spark.sql.types._

class RowSuite extends FunSuite {

  test("create row") {
    val expected = new GenericMutableRow(4)
    expected.update(0, 2147483647)
    expected.update(1, "this is a string")
    expected.update(2, false)
    expected.update(3, null)
    val actual1 = Row(2147483647, "this is a string", false, null)
    assert(expected.size === actual1.size)
    assert(expected.getInt(0) === actual1.getInt(0))
    assert(expected.getString(1) === actual1.getString(1))
    assert(expected.getBoolean(2) === actual1.getBoolean(2))
    assert(expected(3) === actual1(3))

    val actual2 = Row.fromSeq(Seq(2147483647, "this is a string", false, null))
    assert(expected.size === actual2.size)
    assert(expected.getInt(0) === actual2.getInt(0))
    assert(expected.getString(1) === actual2.getString(1))
    assert(expected.getBoolean(2) === actual2.getBoolean(2))
    assert(expected(3) === actual2(3))
  }

  test("SpecificMutableRow.update with null") {
    val row = new SpecificMutableRow(Seq(IntegerType))
    row(0) = null
    assert(row.isNullAt(0))
  }

  test("serialize w/ kryo") {
    val row = Seq((1, Seq(1), Map(1 -> 1), BigDecimal(1))).toDF().first()
    val serializer = new SparkSqlSerializer(TestSQLContext.sparkContext.getConf)
    val instance = serializer.newInstance()
    val ser = instance.serialize(row)
    val de = instance.deserialize(ser).asInstanceOf[Row]
    assert(de === row)
  }
}
