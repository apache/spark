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

import org.apache.spark.SparkException
import org.apache.spark.sql.{functions, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.test.SharedSQLContext

import scala.collection.mutable.ArrayBuffer

class DataFrameCallbackSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  import functions._

  test("execute callback functions when a DataFrame action finished successfully") {
    val metrics = ArrayBuffer.empty[(String, QueryExecution, Long)]
    val listener = new QueryExecutionListener {
      // Only test successful case here, so no need to implement `onFailure`
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        metrics += ((funcName, qe, duration))
      }
    }
    sqlContext.listenerManager.register(listener)

    val df = Seq(1 -> "a").toDF("i", "j")
    df.select("i").collect()
    df.filter($"i" > 0).count()

    assert(metrics.length == 2)

    assert(metrics(0)._1 == "collect")
    assert(metrics(0)._2.analyzed.isInstanceOf[Project])
    assert(metrics(0)._3 > 0)

    assert(metrics(1)._1 == "count")
    assert(metrics(1)._2.analyzed.isInstanceOf[Aggregate])
    assert(metrics(1)._3 > 0)
  }

  test("execute callback functions when a DataFrame action failed") {
    val metrics = ArrayBuffer.empty[(String, QueryExecution, Exception)]
    val listener = new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        metrics += ((funcName, qe, exception))
      }

      // Only test failed case here, so no need to implement `onSuccess`
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {}
    }
    sqlContext.listenerManager.register(listener)

    val errorUdf = udf[Int, Int] { _ => throw new RuntimeException("udf error") }
    val df = sparkContext.makeRDD(Seq(1 -> "a")).toDF("i", "j")

    // Ignore the log when we are expecting an exception.
    sparkContext.setLogLevel("FATAL")
    val e = intercept[SparkException](df.select(errorUdf($"i")).collect())

    assert(metrics.length == 1)
    assert(metrics(0)._1 == "collect")
    assert(metrics(0)._2.analyzed.isInstanceOf[Project])
    assert(metrics(0)._3.getMessage == e.getMessage)
  }
}
