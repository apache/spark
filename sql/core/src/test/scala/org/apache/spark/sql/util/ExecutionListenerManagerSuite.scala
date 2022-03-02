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

package org.apache.spark.sql.util

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark._
import org.apache.spark.sql.{LocalSparkSession, SparkSession}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf._

class ExecutionListenerManagerSuite extends SparkFunSuite with LocalSparkSession {

  test("register query execution listeners using configuration") {
    import CountingQueryExecutionListener._
    val conf = new SparkConf(false)
      .set(QUERY_EXECUTION_LISTENERS, Seq(classOf[CountingQueryExecutionListener].getName()))
    spark = SparkSession.builder().master("local").appName("test").config(conf).getOrCreate()

    spark.sql("select 1").collect()
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(INSTANCE_COUNT.get() === 1)
    assert(CALLBACK_COUNT.get() === 1)

    val cloned = spark.cloneSession()
    cloned.sql("select 1").collect()
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(INSTANCE_COUNT.get() === 1)
    assert(CALLBACK_COUNT.get() === 2)
  }

  test("SPARK-37780: register query execution listeners using SQLCoonf") {
    import CountingSQLConfQueryExecutionListener._
    val conf = new SparkConf(false)
      .setMaster("local")
      .setAppName("test")
      .set(QUERY_EXECUTION_LISTENERS, Seq(classOf[SQLConfQueryExecutionListener].getName()))
      .set("spark.aaa", "aaa")
    val sc = new SparkContext(conf)
    spark = SparkSession.builder()
      .sparkContext(sc)
      .config("spark.bbb", "bbb")
      .getOrCreate()

    spark.sql("select 1").collect()
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(INSTANCE_COUNT.get() === 1)
    assert(CALLBACK_COUNT.get() === 1)

    val cloned = spark.cloneSession()
    cloned.sql("select 1").collect()
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(INSTANCE_COUNT.get() === 1)
    assert(CALLBACK_COUNT.get() === 2)
  }
}

private class CountingQueryExecutionListener extends QueryExecutionListener {

  import CountingQueryExecutionListener._

  INSTANCE_COUNT.incrementAndGet()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    CALLBACK_COUNT.incrementAndGet()
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    CALLBACK_COUNT.incrementAndGet()
  }

}

private object CountingQueryExecutionListener {

  val CALLBACK_COUNT = new AtomicInteger()
  val INSTANCE_COUNT = new AtomicInteger()

}

private class SQLConfQueryExecutionListener extends QueryExecutionListener {
  import CountingSQLConfQueryExecutionListener._
  val sqlConf = SQLConf.get

  assert(sqlConf.getConfString("spark.aaa") == "aaa")
  assert(sqlConf.getConfString("spark.bbb") == "bbb")

  INSTANCE_COUNT.incrementAndGet()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    CALLBACK_COUNT.incrementAndGet()
  }
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    CALLBACK_COUNT.incrementAndGet()
  }
}

private object CountingSQLConfQueryExecutionListener {

  val CALLBACK_COUNT = new AtomicInteger()
  val INSTANCE_COUNT = new AtomicInteger()

}
