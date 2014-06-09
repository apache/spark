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

import org.apache.spark.sql.test._

/* Implicits */
import TestSQLContext._

class SQLConfSuite extends QueryTest {

  val testKey = "test.key.0"
  val testVal = "test.val.0"

  test("programmatic ways of basic setting and getting") {
    assert(getOption(testKey).isEmpty)
    assert(getAll.toSet === Set())

    set(testKey, testVal)
    assert(get(testKey) == testVal)
    assert(get(testKey, testVal + "_") == testVal)
    assert(getOption(testKey) == Some(testVal))
    assert(contains(testKey))

    // Tests SQLConf as accessed from a SQLContext is mutable after
    // the latter is initialized, unlike SparkConf inside a SparkContext.
    assert(TestSQLContext.get(testKey) == testVal)
    assert(TestSQLContext.get(testKey, testVal + "_") == testVal)
    assert(TestSQLContext.getOption(testKey) == Some(testVal))
    assert(TestSQLContext.contains(testKey))

    clear()
  }

  test("parse SQL set commands") {
    sql(s"set $testKey=$testVal")
    assert(get(testKey, testVal + "_") == testVal)
    assert(TestSQLContext.get(testKey, testVal + "_") == testVal)

    sql("set mapred.reduce.tasks=20")
    assert(get("mapred.reduce.tasks", "0") == "20")
    sql("set mapred.reduce.tasks = 40")
    assert(get("mapred.reduce.tasks", "0") == "40")

    val key = "spark.sql.key"
    val vs = "val0,val_1,val2.3,my_table"
    sql(s"set $key=$vs")
    assert(get(key, "0") == vs)

    sql(s"set $key=")
    assert(get(key, "0") == "")

    clear()
  }

}
