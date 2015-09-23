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

import org.apache.spark.sql.test.{TestSQLContext, SharedSQLContext}


class SQLConfSuite extends QueryTest with SharedSQLContext {
  private val testKey = "test.key.0"
  private val testVal = "test.val.0"

  test("propagate from spark conf") {
    // We create a new context here to avoid order dependence with other tests that might call
    // clear().
    val newContext = new SQLContext(sparkContext)
    assert(newContext.getConf("spark.sql.testkey", "false") === "true")
  }

  test("programmatic ways of basic setting and getting") {
    // Set a conf first.
    sqlContext.setConf(testKey, testVal)
    // Clear the conf.
    sqlContext.conf.clear()
    // After clear, only overrideConfs used by unit test should be in the SQLConf.
    assert(sqlContext.getAllConfs === TestSQLContext.overrideConfs)

    sqlContext.setConf(testKey, testVal)
    assert(sqlContext.getConf(testKey) === testVal)
    assert(sqlContext.getConf(testKey, testVal + "_") === testVal)
    assert(sqlContext.getAllConfs.contains(testKey))

    // Tests SQLConf as accessed from a SQLContext is mutable after
    // the latter is initialized, unlike SparkConf inside a SparkContext.
    assert(sqlContext.getConf(testKey) === testVal)
    assert(sqlContext.getConf(testKey, testVal + "_") === testVal)
    assert(sqlContext.getAllConfs.contains(testKey))

    sqlContext.conf.clear()
  }

  test("parse SQL set commands") {
    sqlContext.conf.clear()
    sql(s"set $testKey=$testVal")
    assert(sqlContext.getConf(testKey, testVal + "_") === testVal)
    assert(sqlContext.getConf(testKey, testVal + "_") === testVal)

    sql("set some.property=20")
    assert(sqlContext.getConf("some.property", "0") === "20")
    sql("set some.property = 40")
    assert(sqlContext.getConf("some.property", "0") === "40")

    val key = "spark.sql.key"
    val vs = "val0,val_1,val2.3,my_table"
    sql(s"set $key=$vs")
    assert(sqlContext.getConf(key, "0") === vs)

    sql(s"set $key=")
    assert(sqlContext.getConf(key, "0") === "")

    sqlContext.conf.clear()
  }

  test("deprecated property") {
    sqlContext.conf.clear()
    val original = sqlContext.conf.numShufflePartitions
    try{
      sql(s"set ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS}=10")
      assert(sqlContext.conf.numShufflePartitions === 10)
    } finally {
      sql(s"set ${SQLConf.SHUFFLE_PARTITIONS}=$original")
    }
  }

  test("invalid conf value") {
    sqlContext.conf.clear()
    val e = intercept[IllegalArgumentException] {
      sql(s"set ${SQLConf.CASE_SENSITIVE.key}=10")
    }
    assert(e.getMessage === s"${SQLConf.CASE_SENSITIVE.key} should be boolean, but was 10")
  }
}
