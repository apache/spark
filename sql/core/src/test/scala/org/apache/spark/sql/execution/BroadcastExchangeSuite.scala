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

package org.apache.spark.sql.execution

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.SparkException
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.HashedRelation
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class BroadcastExchangeSuite extends SparkPlanTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("BroadcastExchange should cancel the job group if timeout") {
    val startLatch = new CountDownLatch(1)
    val endLatch = new CountDownLatch(1)
    var jobEvents: Seq[SparkListenerEvent] = Seq.empty[SparkListenerEvent]
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        jobEvents :+= jobEnd
        endLatch.countDown()
      }
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        jobEvents :+= jobStart
        startLatch.countDown()
      }
    })

    withSQLConf(SQLConf.BROADCAST_TIMEOUT.key -> "0") {
      val df = spark.range(100).join(spark.range(15).as[Long].map { x =>
        Thread.sleep(5000)
        x
      }).where("id = value")

      // get the exchange physical plan
      val hashExchange = collect(
        df.queryExecution.executedPlan) { case p: BroadcastExchangeExec => p }.head

      // materialize the future and wait for the job being scheduled
      hashExchange.prepare()
      startLatch.await(5, TimeUnit.SECONDS)

      // check timeout exception is captured by just executing the exchange
      val hashEx = intercept[SparkException] {
        hashExchange.executeBroadcast[HashedRelation]()
      }
      assert(hashEx.getMessage.contains("Could not execute broadcast"))

      // wait for cancel is posted and then check the results.
      endLatch.await(5, TimeUnit.SECONDS)
      assert(jobCancelled())
    }

    def jobCancelled(): Boolean = {
      val events = jobEvents.toArray
      val hasStart = events(0).isInstanceOf[SparkListenerJobStart]
      val hasCancelled = events(1).asInstanceOf[SparkListenerJobEnd].jobResult
        .asInstanceOf[JobFailed].exception.getMessage.contains("cancelled job group")
      events.length == 2 && hasStart && hasCancelled
    }
  }

  test("set broadcastTimeout to -1") {
    withSQLConf(SQLConf.BROADCAST_TIMEOUT.key -> "-1") {
      val df = spark.range(1).toDF()
      val joinDF = df.join(broadcast(df), "id")
      val broadcastExchangeExec = collect(
        joinDF.queryExecution.executedPlan) { case p: BroadcastExchangeExec => p }
      assert(broadcastExchangeExec.size == 1, "one and only BroadcastExchangeExec")
      assert(joinDF.collect().length == 1)
    }
  }
}
