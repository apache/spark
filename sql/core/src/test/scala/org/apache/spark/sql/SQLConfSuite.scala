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

  val conf = TestSQLContext.sqlConf
  val testKey = "test.key.0"
  val testVal = "test.val.0"

  test("programmatic ways of basic setting and getting") {
    assert(conf != null)
    assert(conf.getOption(testKey).isEmpty)
    assert(conf.getAll.toSet === Set())

    conf.set(testKey, testVal)
    assert(conf.get(testKey) == testVal)
    assert(conf.get(testKey, testVal + "_") == testVal)
    assert(conf.getOption(testKey) == Some(testVal))
    assert(conf.contains(testKey))

    // Tests SQLConf as accessed from a SQLContext is mutable after
    // the latter is initialized, unlike SparkConf inside a SparkContext.
    assert(TestSQLContext.sqlConf.get(testKey) == testVal)
    assert(TestSQLContext.sqlConf.get(testKey, testVal + "_") == testVal)
    assert(TestSQLContext.sqlConf.getOption(testKey) == Some(testVal))
    assert(TestSQLContext.sqlConf.contains(testKey))

    conf.clear()
  }

  test("SQLConf picks up SQL set commands") {
    sql(s"set $testKey=$testVal")
    assert(conf.get(testKey, testVal + "_") == testVal)
    assert(TestSQLContext.sqlConf.get(testKey, testVal + "_") == testVal)

    sql("set mapred.reduce.tasks=20")
    assert(conf.get("mapred.reduce.tasks", "0") == "20")
    sql("set mapred.reduce.tasks = 40")
    assert(conf.get("mapred.reduce.tasks", "0") == "40")
  }

}
