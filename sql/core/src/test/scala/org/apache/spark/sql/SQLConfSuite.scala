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

import org.scalatest.FunSuiteLike

import org.apache.spark.sql.test._

/* Implicits */
import TestSQLContext._

class SQLConfSuite extends QueryTest with FunSuiteLike {

  val testKey = "test.key.0"
  val testVal = "test.val.0"

  test("propagate from spark conf") {
    // We create a new context here to avoid order dependence with other tests that might call
    // clear().
    val newContext = new SQLContext(TestSQLContext.sparkContext)
    assert(newContext.getConf("spark.sql.testkey", "false") == "true")
  }

  test("programmatic ways of basic setting and getting") {
    conf.clear()
    assert(getAllConfs.size === 0)

    setConf(testKey, testVal)
    assert(getConf(testKey) == testVal)
    assert(getConf(testKey, testVal + "_") == testVal)
    assert(getAllConfs.contains(testKey))

    // Tests SQLConf as accessed from a SQLContext is mutable after
    // the latter is initialized, unlike SparkConf inside a SparkContext.
    assert(TestSQLContext.getConf(testKey) == testVal)
    assert(TestSQLContext.getConf(testKey, testVal + "_") == testVal)
    assert(TestSQLContext.getAllConfs.contains(testKey))

    conf.clear()
  }

  test("parse SQL set commands") {
    conf.clear()
    sql(s"set $testKey=$testVal")
    assert(getConf(testKey, testVal + "_") == testVal)
    assert(TestSQLContext.getConf(testKey, testVal + "_") == testVal)

    sql("set some.property=20")
    assert(getConf("some.property", "0") == "20")
    sql("set some.property = 40")
    assert(getConf("some.property", "0") == "40")

    val key = "spark.sql.key"
    val vs = "val0,val_1,val2.3,my_table"
    sql(s"set $key=$vs")
    assert(getConf(key, "0") == vs)

    sql(s"set $key=")
    assert(getConf(key, "0") == "")

    conf.clear()
  }

  test("deprecated property") {
    conf.clear()
    sql(s"set ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS}=10")
    assert(getConf(SQLConf.SHUFFLE_PARTITIONS) == "10")
  }
}
