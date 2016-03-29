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

import org.apache.spark.{SharedSparkContext, SparkFunSuite}

class SQLContextSuite extends SparkFunSuite with SharedSparkContext{

  test("getOrCreate instantiates SQLContext") {
    val sqlContext = SQLContext.getOrCreate(sc)
    assert(sqlContext != null, "SQLContext.getOrCreate returned null")
    assert(SQLContext.getOrCreate(sc).eq(sqlContext),
      "SQLContext created by SQLContext.getOrCreate not returned by SQLContext.getOrCreate")
  }

  test("getOrCreate return the original SQLContext") {
    val sqlContext = SQLContext.getOrCreate(sc)
    val newSession = sqlContext.newSession()
    assert(SQLContext.getOrCreate(sc).eq(sqlContext),
      "SQLContext.getOrCreate after explicitly created SQLContext did not return the context")
    SQLContext.setActive(newSession)
    assert(SQLContext.getOrCreate(sc).eq(newSession),
      "SQLContext.getOrCreate after explicitly setActive() did not return the active context")
  }

  test("Sessions of SQLContext") {
    val sqlContext = SQLContext.getOrCreate(sc)
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

  test("SPARK-13390: createDataFrame(java.util.List[_],Class[_]) NotSerializableException") {
    val rows = new java.util.ArrayList[IntJavaBean]()
    rows.add(new IntJavaBean(1))
    val sqlContext = SQLContext.getOrCreate(sc)
    // Without the fix for SPARK-13390, this will throw NotSerializableException
    sqlContext.createDataFrame(rows, classOf[IntJavaBean]).groupBy("int").count().collect()
  }
}

class IntJavaBean(private var i: Int) extends Serializable {

  def getInt(): Int = i

  def setInt(i: Int): Unit = {
    this.i = i
  }
}
