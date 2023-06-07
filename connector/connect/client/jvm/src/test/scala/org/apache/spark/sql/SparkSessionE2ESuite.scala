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

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import org.scalatest.concurrent.Eventually._

import org.apache.spark.sql.connect.client.util.RemoteSparkSession
import org.apache.spark.util.ThreadUtils

/**
 * Warning: SPARK-43648 moves two test cases related to `interrupt all` from
 * `ClientE2ETestSuite` to the current test class to avoid the maven test issue
 * of missing `org.apache.spark.sql.connect.client.SparkResult` during udf
 * deserialization in server side. So please don't import classes that only
 * exist in `spark-connect-client-jvm.jar` into the this class, as it will
 * trigger similar maven test failures again.
 */
class SparkSessionE2ESuite extends RemoteSparkSession {

  test("interrupt all - background queries, foreground interrupt") {
    val session = spark
    import session.implicits._
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global
    val q1 = Future {
      spark.range(10).map(n => { Thread.sleep(30000); n }).collect()
    }
    val q2 = Future {
      spark.range(10).map(n => { Thread.sleep(30000); n }).collect()
    }
    var q1Interrupted = false
    var q2Interrupted = false
    var error: Option[String] = None
    q1.onComplete {
      case Success(_) =>
        error = Some("q1 shouldn't have finished!")
      case Failure(t) if t.getMessage.contains("cancelled") =>
        q1Interrupted = true
      case Failure(t) =>
        error = Some("unexpected failure in q1: " + t.toString)
    }
    q2.onComplete {
      case Success(_) =>
        error = Some("q2 shouldn't have finished!")
      case Failure(t) if t.getMessage.contains("cancelled") =>
        q2Interrupted = true
      case Failure(t) =>
        error = Some("unexpected failure in q2: " + t.toString)
    }
    // 20 seconds is < 30 seconds the queries should be running,
    // because it should be interrupted sooner
    eventually(timeout(20.seconds), interval(1.seconds)) {
      // keep interrupting every second, until both queries get interrupted.
      spark.interruptAll()
      assert(error.isEmpty, s"Error not empty: $error")
      assert(q1Interrupted)
      assert(q2Interrupted)
    }
  }

  test("interrupt all - foreground queries, background interrupt") {
    val session = spark
    import session.implicits._
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global

    @volatile var finished = false
    val interruptor = Future {
      eventually(timeout(20.seconds), interval(1.seconds)) {
        spark.interruptAll()
        assert(finished)
      }
      finished
    }
    val e1 = intercept[io.grpc.StatusRuntimeException] {
      spark.range(10).map(n => { Thread.sleep(30.seconds.toMillis); n }).collect()
    }
    assert(e1.getMessage.contains("cancelled"), s"Unexpected exception: $e1")
    val e2 = intercept[io.grpc.StatusRuntimeException] {
      spark.range(10).map(n => { Thread.sleep(30.seconds.toMillis); n }).collect()
    }
    assert(e2.getMessage.contains("cancelled"), s"Unexpected exception: $e2")
    finished = true
    assert(ThreadUtils.awaitResult(interruptor, 10.seconds))
  }
}
