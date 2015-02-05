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
import org.apache.spark.sql.TestData._
import org.apache.spark.sql.sources._
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

  test("SPARK-2789 applyNames RDD to DataFrame") {
    val rdd1 = unparsedStrings.map { r =>
      val values = r.split(",").map(_.trim)
      val v5 = try values(3).toInt catch {
        case _: NumberFormatException => 0
      }
      Seq(values(0).toInt, values(1), values(2).toBoolean, v5)
    }
    val newContext = new SQLContext(TestSQLContext.sparkContext)
    val applyNames1 = newContext.applyNames("a b c d", rdd1)
    assert(applyNames1.isInstanceOf[DataFrame])
    applyNames1.registerTempTable("applyNames1")
    
    checkAnswer(
      newContext.sql("SELECT * FROM applyNames1"),
      Row(1, "A1", true, 0) ::
      Row(2, "B2", false, 0) ::
      Row(3, "C3", true, 0) ::
      Row(4, "D4", true, 2147483644) :: Nil)

      val e = intercept[DDLException](newContext.applyNames("a in b d", rdd1))
      assert(e.getMessage.startsWith("Reserved words not allowed"))

      val rdd2 = unparsedStrings.map { r =>
        val values = r.split(",").map(_.trim)
        val v5 = try values(3).toInt catch {
          case _: NumberFormatException => 0
        }
        Seq(values(0).toInt, values(1), values(2).toBoolean, Map("k" -> values(0).toInt), Map(values(1) -> values(0).toInt))
      }

      val applyNames2 = newContext.applyNames("a b c d e", rdd2)
      assert(applyNames2.isInstanceOf[DataFrame])
      applyNames2.registerTempTable("applyNames2")
      checkAnswer(newContext.sql("SELECT d['k'] FROM applyNames2"),
      Row(1) ::
      Row(2) ::
      Row(3) ::
      Row(4) :: Nil)
      checkAnswer(newContext.sql("SELECT e['A1'] FROM applyNames2"),
      Row(1) ::
      Row(null) ::
      Row(null) ::
      Row(null) :: Nil)

  }
}
