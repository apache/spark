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

import java.util.concurrent.Executors

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import org.scalatest.concurrent.Eventually._

import org.apache.spark.SparkException
import org.apache.spark.sql.test.RemoteSparkSession
import org.apache.spark.util.SparkThreadUtils.awaitResult

/**
 * NOTE: Do not import classes that only exist in `spark-connect-client-jvm.jar` into the this
 * class, whether explicit or implicit, as it will trigger a UDF deserialization error during
 * Maven build/test.
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
      case Failure(t) if t.getMessage.contains("OPERATION_CANCELED") =>
        q1Interrupted = true
      case Failure(t) =>
        error = Some("unexpected failure in q1: " + t.toString)
    }
    q2.onComplete {
      case Success(_) =>
        error = Some("q2 shouldn't have finished!")
      case Failure(t) if t.getMessage.contains("OPERATION_CANCELED") =>
        q2Interrupted = true
      case Failure(t) =>
        error = Some("unexpected failure in q2: " + t.toString)
    }
    // 20 seconds is < 30 seconds the queries should be running,
    // because it should be interrupted sooner
    val interrupted = mutable.ListBuffer[String]()
    eventually(timeout(20.seconds), interval(1.seconds)) {
      // keep interrupting every second, until both queries get interrupted.
      val ids = spark.interruptAll()
      interrupted ++= ids
      assert(error.isEmpty, s"Error not empty: $error")
      assert(q1Interrupted)
      assert(q2Interrupted)
    }
    assert(interrupted.length == 2, s"Interrupted operations: $interrupted.")
  }

  test("interrupt all - foreground queries, background interrupt") {
    val session = spark
    import session.implicits._
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global

    @volatile var finished = false
    val interrupted = mutable.ListBuffer[String]()

    val interruptor = Future {
      eventually(timeout(20.seconds), interval(1.seconds)) {
        val ids = spark.interruptAll()
        interrupted ++= ids
        assert(finished)
      }
      finished
    }
    val e1 = intercept[SparkException] {
      spark.range(10).map(n => { Thread.sleep(30.seconds.toMillis); n }).collect()
    }
    assert(e1.getMessage.contains("OPERATION_CANCELED"), s"Unexpected exception: $e1")
    val e2 = intercept[SparkException] {
      spark.range(10).map(n => { Thread.sleep(30.seconds.toMillis); n }).collect()
    }
    assert(e2.getMessage.contains("OPERATION_CANCELED"), s"Unexpected exception: $e2")
    finished = true
    assert(awaitResult(interruptor, 10.seconds))
    assert(interrupted.length == 2, s"Interrupted operations: $interrupted.")
  }

  // TODO(SPARK-48139): Re-enable `SparkSessionE2ESuite.interrupt tag`
  ignore("interrupt tag") {
    val session = spark
    import session.implicits._

    // global ExecutionContext has only 2 threads in Apache Spark CI
    // create own thread pool for four Futures used in this test
    val numThreads = 4
    val fpool = Executors.newFixedThreadPool(numThreads)
    val executionContext = ExecutionContext.fromExecutorService(fpool)

    val q1 = Future {
      assert(spark.getTags() == Set())
      spark.addTag("two")
      assert(spark.getTags() == Set("two"))
      spark.clearTags() // check that clearing all tags works
      assert(spark.getTags() == Set())
      spark.addTag("one")
      assert(spark.getTags() == Set("one"))
      try {
        spark
          .range(10)
          .map(n => {
            Thread.sleep(30000); n
          })
          .collect()
      } finally {
        spark.clearTags() // clear for the case of thread reuse by another Future
      }
    }(executionContext)
    val q2 = Future {
      assert(spark.getTags() == Set())
      spark.addTag("one")
      spark.addTag("two")
      spark.addTag("one")
      spark.addTag("two") // duplicates shouldn't matter
      assert(spark.getTags() == Set("one", "two"))
      try {
        spark
          .range(10)
          .map(n => {
            Thread.sleep(30000); n
          })
          .collect()
      } finally {
        spark.clearTags() // clear for the case of thread reuse by another Future
      }
    }(executionContext)
    val q3 = Future {
      assert(spark.getTags() == Set())
      spark.addTag("foo")
      spark.removeTag("foo")
      assert(spark.getTags() == Set()) // check that remove works removing the last tag
      spark.addTag("two")
      assert(spark.getTags() == Set("two"))
      try {
        spark
          .range(10)
          .map(n => {
            Thread.sleep(30000); n
          })
          .collect()
      } finally {
        spark.clearTags() // clear for the case of thread reuse by another Future
      }
    }(executionContext)
    val q4 = Future {
      assert(spark.getTags() == Set())
      spark.addTag("one")
      spark.addTag("two")
      spark.addTag("two")
      assert(spark.getTags() == Set("one", "two"))
      spark.removeTag("two") // check that remove works, despite duplicate add
      assert(spark.getTags() == Set("one"))
      try {
        spark
          .range(10)
          .map(n => {
            Thread.sleep(30000); n
          })
          .collect()
      } finally {
        spark.clearTags() // clear for the case of thread reuse by another Future
      }
    }(executionContext)
    val interrupted = mutable.ListBuffer[String]()

    // q2 and q3 should be cancelled
    interrupted.clear()
    eventually(timeout(20.seconds), interval(1.seconds)) {
      val ids = spark.interruptTag("two")
      interrupted ++= ids
      assert(interrupted.length == 2, s"Interrupted operations: $interrupted.")
    }
    val e2 = intercept[SparkException] {
      awaitResult(q2, 1.minute)
    }
    assert(e2.getCause.getMessage contains "OPERATION_CANCELED")
    val e3 = intercept[SparkException] {
      awaitResult(q3, 1.minute)
    }
    assert(e3.getCause.getMessage contains "OPERATION_CANCELED")
    assert(interrupted.length == 2, s"Interrupted operations: $interrupted.")

    // q1 and q4 should be cancelled
    interrupted.clear()
    eventually(timeout(20.seconds), interval(1.seconds)) {
      val ids = spark.interruptTag("one")
      interrupted ++= ids
      assert(interrupted.length == 2, s"Interrupted operations: $interrupted.")
    }
    val e1 = intercept[SparkException] {
      awaitResult(q1, 1.minute)
    }
    assert(e1.getCause.getMessage contains "OPERATION_CANCELED")
    val e4 = intercept[SparkException] {
      awaitResult(q4, 1.minute)
    }
    assert(e4.getCause.getMessage contains "OPERATION_CANCELED")
    assert(interrupted.length == 2, s"Interrupted operations: $interrupted.")
  }

  test("interrupt operation") {
    val session = spark
    import session.implicits._

    val result = spark
      .range(10)
      .map(n => {
        Thread.sleep(5000); n
      })
      .collectResult()
    // cancel
    val operationId = result.operationId
    val canceledId = spark.interruptOperation(operationId)
    assert(canceledId == Seq(operationId))
    // and check that it got canceled
    val e = intercept[SparkException] {
      result.toArray
    }
    assert(e.getMessage contains "OPERATION_CANCELED")
  }

  test("option propagation") {
    val remote = s"sc://localhost:$serverPort"
    val session1 = SparkSession
      .builder()
      .remote(remote)
      .config("foo", 12L)
      .config("bar", value = true)
      .config("bob", 12.0)
      .config("heading", "north")
      .getOrCreate()
    assert(session1.conf.get("foo") == "12")
    assert(session1.conf.get("bar") == "true")
    assert(session1.conf.get("bob") == String.valueOf(12.0))
    assert(session1.conf.get("heading") == "north")

    // Check if new options are applied to an existing session.
    val session2 = SparkSession
      .builder()
      .remote(remote)
      .config("heading", "south")
      .getOrCreate()
    assert(session2 == session1)
    assert(session2.conf.get("heading") == "south")

    // Create a completely different session, confs are not support to leak.
    val session3 = SparkSession
      .builder()
      .remote(remote)
      .config(Map("foo" -> "13", "baar" -> "false", "heading" -> "east"))
      .create()
    assert(session3 != session1)
    assert(session3.conf.get("foo") == "13")
    assert(session3.conf.get("baar") == "false")
    assert(session3.conf.getOption("bob").isEmpty)
    assert(session3.conf.get("heading") == "east")

    // Try to set a static conf.
    intercept[Exception] {
      SparkSession
        .builder()
        .remote(remote)
        .config("spark.sql.globalTempDatabase", "not_gonna_happen")
        .create()
    }
  }
}
