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

import org.apache.spark.sql.connect.client.util.RemoteSparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ClientE2ETestSuite extends RemoteSparkSession {

  // Spark Result
  test("test spark result schema") {
    val df = spark.sql("select val from (values ('Hello'), ('World')) as t(val)")
    val schema = df.collectResult().schema
    assert(schema == StructType(StructField("val", StringType, false) :: Nil))
  }

  test("test spark result array") {
    val df = spark.sql("select val from (values ('Hello'), ('World')) as t(val)")
    val result = df.collectResult()
    assert(result.length == 2)
    val array = result.toArray
    assert(array.length == 2)
    assert(array(0).getString(0) == "Hello")
    assert(array(1).getString(0) == "World")
  }

  test("simple dataset test") {
    val df = spark.range(10).limit(3)
    val result = df.collectResult()
    assert(result.length == 3)
    val array = result.toArray
    assert(array(0).getLong(0) == 0)
    assert(array(1).getLong(0) == 1)
    assert(array(2).getLong(0) == 2)
  }

  test("simple udf test") {

    def dummyUdf(x: Int): Int = x + 5
    val myUdf = udf(dummyUdf _)
    val df = spark.range(5).select(myUdf(Column("id")))

    val result = df.collectResult()
    assert(result.length == 5)
    result.toArray.zipWithIndex.foreach { case (v, idx) =>
      assert(v.getInt(0) == idx + 5)
    }
  }

  // TODO test large result when we can create table or view
  // test("test spark large result")
}
