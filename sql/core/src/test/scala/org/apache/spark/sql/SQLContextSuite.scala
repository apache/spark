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
import org.apache.spark.sql.test.SharedSQLContext

class SQLContextSuite extends SparkFunSuite with SharedSQLContext {

  override def afterAll(): Unit = {
    try {
      SQLContext.setInstantiatedContext(sqlContext)
    } finally {
      super.afterAll()
    }
  }

  test("getOrCreate instantiates SQLContext") {
    SQLContext.clearInstantiatedContext()
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    assert(sqlContext != null, "SQLContext.getOrCreate returned null")
    assert(SQLContext.getOrCreate(sparkContext).eq(sqlContext),
      "SQLContext created by SQLContext.getOrCreate not returned by SQLContext.getOrCreate")
  }

  test("getOrCreate gets last explicitly instantiated SQLContext") {
    SQLContext.clearInstantiatedContext()
    val sqlContext = new SQLContext(sparkContext)
    assert(SQLContext.getOrCreate(sparkContext) != null,
      "SQLContext.getOrCreate after explicitly created SQLContext returned null")
    assert(SQLContext.getOrCreate(sparkContext).eq(sqlContext),
      "SQLContext.getOrCreate after explicitly created SQLContext did not return the context")
  }

  test("getOrCreate return the original SQLContext") {
    SQLContext.clearInstantiatedContext()
    val sqlContext = new SQLContext(sparkContext)
    val newSession = sqlContext.newSession()
    assert(SQLContext.getOrCreate(sparkContext).eq(sqlContext),
      "SQLContext.getOrCreate after explicitly created SQLContext did not return the context")
    SQLContext.setActive(newSession)
    assert(SQLContext.getOrCreate(sparkContext).eq(newSession),
      "SQLContext.getOrCreate after explicitly setActive() did not return the active context")
  }

  test("Sessions of SQLContext") {
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    val session1 = sqlContext.newSession()
    val session2 = sqlContext.newSession()

    // all have the default configurations
    val key = SQLConf.SHUFFLE_PARTITIONS.key
    assert(session1.getConf(key) === session2.getConf(key))
    session1.setConf(key, "1")
    session2.setConf(key, "2")
    assert(session1.getConf(key) === "1")
    assert(session2.getConf(key) === "2")

    // temporary table should not be shared
    val df = session1.range(10)
    df.registerTempTable("test1")
    assert(session1.tableNames().contains("test1"))
    assert(!session2.tableNames().contains("test1"))

    // UDF should not be shared
    def myadd(a: Int, b: Int): Int = a + b
    session1.udf.register[Int, Int, Int]("myadd", myadd)
    session1.sql("select myadd(1, 2)").explain()
    intercept[AnalysisException] {
      session2.sql("select myadd(1, 2)").explain()
    }
  }
}
