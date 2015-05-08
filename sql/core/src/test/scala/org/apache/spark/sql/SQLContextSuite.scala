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

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.{SparkConf, SparkContext}

class SQLContextSuite extends FunSuite with BeforeAndAfter {
  private val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("SQLContextSuite")
    .clone.set("newContext", "true")

  private var sparkContext: SparkContext = null
  private var sqlContext: SQLContext = null

  before {
    SQLContext.clearLastInstantiatedContext()
    SparkContext.clearActiveContext()
  }

  after {
    if (sqlContext != null) {
      sparkContext = sqlContext.sparkContext
      sqlContext = null
    }
    if (sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
    }
  }

  test("getOrCreate instantiates SQLContext from SparkConf") {
    sqlContext = SQLContext.getOrCreate(sparkConf)
    assert(sqlContext != null, "SQLContext not created")
    assert(sqlContext.sparkContext != null, "SparkContext not created")
    assert(sqlContext.sparkContext.conf.getBoolean("newContext", false),
      "Provided conf not used to create SparkContext")
    assert(SQLContext.getOrCreate(sparkConf).eq(sqlContext),
      "Created SQLContext not saved as singleton")
  }

  test("getOrCreate instantiates SQLContext from SparkContext") {
    sparkContext = new SparkContext(sparkConf)
    sqlContext = SQLContext.getOrCreate(sparkConf)
    assert(sqlContext != null, "SQLContext not created")
    assert(sqlContext.sparkContext != null, "SparkContext not passed")
    assert(SQLContext.getOrCreate(sparkConf) != null,
      "Instantiated SQLContext was not saved, null returned")
    assert(SQLContext.getOrCreate(sparkConf).eq(sqlContext),
      "Different SQLContext was returned")
  }

  test("getOrCreate gets last explicitly instantiated SQLContext") {
    sparkContext = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sparkContext)
    assert(SQLContext.getOrCreate(sparkConf) != null,
      "Explicitly instantiated SQLContext was not saved, null returned")
    assert(SQLContext.getOrCreate(sparkConf).eq(sqlContext),
      "Different SQLContext was returned")
  }
}
