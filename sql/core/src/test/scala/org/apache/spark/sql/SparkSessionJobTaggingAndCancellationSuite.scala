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

import java.util.concurrent.{ConcurrentHashMap, Executors, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.{LocalSparkContext, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.ThreadUtils

/**
 * Test cases for the tagging and cancellation APIs provided by [[SparkSession]].
 */
@ExtendedSQLTest
class SparkSessionJobTaggingAndCancellationSuite
  extends SparkFunSuite
  with Eventually
  with LocalSparkContext {

  override def afterEach(): Unit = {
    try {
      // This suite should not interfere with the other test suites.
      classic.SparkSession.getActiveSession.foreach(_.stop())
      classic.SparkSession.clearActiveSession()
      classic.SparkSession.getDefaultSession.foreach(_.stop())
      classic.SparkSession.clearDefaultSession()
      resetSparkContext()
    } finally {
      super.afterEach()
    }
  }

  test("Tags are not inherited by new sessions") {
    val session = classic.SparkSession.builder().master("local").getOrCreate()

    assert(session.getTags() == Set())
    session.addTag("one")
    assert(session.getTags() == Set("one"))

    val newSession = session.newSession()
    assert(newSession.getTags() == Set())
  }

  test("Tags are inherited by cloned sessions") {
    val session = classic.SparkSession.builder().master("local").getOrCreate()

    assert(session.getTags() == Set())
    session.addTag("one")
    assert(session.getTags() == Set("one"))

    val clonedSession = session.cloneSession()
    assert(clonedSession.getTags() == Set("one"))
    clonedSession.addTag("two")
    assert(clonedSession.getTags() == Set("one", "two"))

    // Tags are not propagated back to the original session
    assert(session.getTags() == Set("one"))
  }

  test("Tags set from session are prefixed with session UUID") {
    sc = new SparkContext("local[2]", "test")
    val session = classic.SparkSession.builder().sparkContext(sc).getOrCreate()
    import session.implicits._

    val sem = new Semaphore(0)
    sc.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        sem.release()
      }
    })

    session.addTag("one")
    Future {
      session.range(1, 10000).map { i => Thread.sleep(100); i }.count()
    }(ExecutionContext.global)

    assert(sem.tryAcquire(1, 1, TimeUnit.MINUTES))
    val activeJobsFuture =
      session.sparkContext.cancelJobsWithTagWithFuture(
        session.managedJobTags.get()("one"), "reason")
    val activeJob = ThreadUtils.awaitResult(activeJobsFuture, 60.seconds).head
    val actualTags = activeJob.properties.getProperty(SparkContext.SPARK_JOB_TAGS)
      .split(SparkContext.SPARK_JOB_TAGS_SEP)
    assert(actualTags.toSet == Set(
      session.sessionJobTag,
      s"${session.sessionJobTag}-thread-${session.threadUuid.get()}-one",
      SQLExecution.executionIdJobTag(
        session,
        activeJob.properties.get(SQLExecution.EXECUTION_ROOT_ID_KEY).asInstanceOf[String].toLong)))
  }

  // TODO(SPARK-50205): Re-enable this test case.
  ignore("Cancellation APIs in SparkSession are isolated") {
    sc = new SparkContext("local[2]", "test")
    val globalSession = classic.SparkSession.builder().sparkContext(sc).getOrCreate()
    var sessionA: classic.SparkSession = null
    var sessionB: classic.SparkSession = null
    var sessionC: classic.SparkSession = null
    var (threadUuidA, threadUuidB, threadUuidC): (String, String, String) = (null, null, null)

    // global ExecutionContext has only 2 threads in Apache Spark CI
    // create own thread pool for four Futures used in this test
    val threadPool = Executors.newFixedThreadPool(3)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

    try {
      // Add a listener to release the semaphore once jobs are launched.
      val sem = new Semaphore(0)
      val jobEnded = new AtomicInteger(0)
      val jobProperties: ConcurrentHashMap[Int, java.util.Properties] = new ConcurrentHashMap()

      sc.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          jobProperties.put(jobStart.jobId, jobStart.properties)
          sem.release()
        }

        override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
          sem.release()
          jobEnded.incrementAndGet()
        }
      })

      var realTagOneForSessionA: String = null
      var childThread: Thread = null
      val childThreadLock = new Semaphore(0)

      // Note: since tags are added in the Future threads, they don't need to be cleared in between.
      val jobA = Future {
        sessionA = globalSession.cloneSession()
        import globalSession.implicits._

        threadUuidA = sessionA.threadUuid.get()
        assert(sessionA.getTags() == Set())
        sessionA.addTag("two")
        assert(sessionA.getTags() == Set("two"))
        sessionA.clearTags() // check that clearing all tags works
        assert(sessionA.getTags() == Set())
        sessionA.addTag("one")
        realTagOneForSessionA = sessionA.managedJobTags.get()("one")
        assert(realTagOneForSessionA ==
          s"${sessionA.sessionJobTag}-thread-${sessionA.threadUuid.get()}-one")
        assert(sessionA.getTags() == Set("one"))

        // Create a child thread which inherits thread-local variables and tries to interrupt
        // the job started from the parent thread. The child thread is blocked until the main
        // thread releases the lock.
        childThread = new Thread {
          override def run(): Unit = {
            assert(childThreadLock.tryAcquire(1, 20, TimeUnit.SECONDS))
            assert(sessionA.getTags() == Set("one"))
            assert(sessionA.interruptTag("one").size == 1)
          }
        }
        childThread.start()
        try {
          sessionA.range(1, 10000).map { i => Thread.sleep(100); i }.count()
        } finally {
          childThread.interrupt()
          sessionA.clearTags() // clear for the case of thread reuse by another Future
        }
      }
      val jobB = Future {
        sessionB = globalSession.cloneSession()
        import globalSession.implicits._

        threadUuidB = sessionB.threadUuid.get()
        assert(sessionB.getTags() == Set())
        sessionB.addTag("one")
        sessionB.addTag("two")
        sessionB.addTag("one")
        sessionB.addTag("two") // duplicates shouldn't matter
        assert(sessionB.getTags() == Set("one", "two"))
        try {
          sessionB.range(1, 10000, 2).map { i => Thread.sleep(100); i }.count()
        } finally {
          sessionB.clearTags() // clear for the case of thread reuse by another Future
        }
      }
      val jobC = Future {
        sessionC = globalSession.cloneSession()
        import globalSession.implicits._

        threadUuidC = sessionC.threadUuid.get()
        sessionC.addTag("foo")
        sessionC.removeTag("foo")
        assert(sessionC.getTags() == Set()) // check that remove works removing the last tag
        sessionC.addTag("boo")
        try {
          sessionC.range(1, 10000, 2).map { i => Thread.sleep(100); i }.count()
        } finally {
          sessionC.clearTags() // clear for the case of thread reuse by another Future
        }
      }

      // Block until four jobs have started.
      assert(sem.tryAcquire(3, 1, TimeUnit.MINUTES))

      // Tags are applied
      def realUserTag(s: String, t: String, ta: String): String = s"spark-session-$s-thread-$t-$ta"
      assert(jobProperties.size == 3)
      for (ss <- Seq(sessionA, sessionB, sessionC)) {
        val jobProperty = jobProperties.values().asScala.filter(_.get(SparkContext.SPARK_JOB_TAGS)
          .asInstanceOf[String].contains(ss.sessionUUID))
        assert(jobProperty.size == 1)
        val tags = jobProperty.head.get(SparkContext.SPARK_JOB_TAGS).asInstanceOf[String]
          .split(SparkContext.SPARK_JOB_TAGS_SEP)

        val executionRootIdTag = SQLExecution.executionIdJobTag(
          ss,
          jobProperty.head.get(SQLExecution.EXECUTION_ROOT_ID_KEY).asInstanceOf[String].toLong)

        ss match {
          case s if s == sessionA => assert(tags.toSet == Set(
            s.sessionJobTag, executionRootIdTag, realUserTag(s.sessionUUID, threadUuidA, "one")))
          case s if s == sessionB => assert(tags.toSet == Set(
            s.sessionJobTag,
            executionRootIdTag,
            realUserTag(s.sessionUUID, threadUuidB, "one"),
            realUserTag(s.sessionUUID, threadUuidB, "two")))
          case s if s == sessionC => assert(tags.toSet == Set(
            s.sessionJobTag, executionRootIdTag, realUserTag(s.sessionUUID, threadUuidC, "boo")))
        }
      }

      // Global session cancels nothing
      assert(globalSession.interruptAll().isEmpty)
      assert(globalSession.interruptTag("one").isEmpty)
      assert(globalSession.interruptTag("two").isEmpty)
      for (i <- SQLExecution.executionIdToQueryExecution.keys().asScala) {
        assert(globalSession.interruptOperation(i.toString).isEmpty)
      }
      assert(jobEnded.intValue == 0)

      // One job cancelled
      for (i <- SQLExecution.executionIdToQueryExecution.keys().asScala) {
        sessionC.interruptOperation(i.toString)
      }
      val eC = intercept[SparkException] {
        ThreadUtils.awaitResult(jobC, 1.minute)
      }.getCause
      assert(eC.getMessage contains "cancelled")
      assert(sem.tryAcquire(1, 1, TimeUnit.MINUTES))
      assert(jobEnded.intValue == 1)

      // Another job cancelled. The next line cancels nothing because we're now in another thread.
      // The real cancel is done through unblocking a child thread, which is waiting for a lock
      assert(sessionA.interruptTag("one").isEmpty)
      childThreadLock.release()
      val eA = intercept[SparkException] {
        ThreadUtils.awaitResult(jobA, 1.minute)
      }.getCause
      assert(eA.getMessage contains "cancelled job tags one")
      assert(sem.tryAcquire(1, 1, TimeUnit.MINUTES))
      assert(jobEnded.intValue == 2)

      // The last job cancelled
      sessionB.interruptAll()
      val eB = intercept[SparkException] {
        ThreadUtils.awaitResult(jobB, 1.minute)
      }.getCause
      assert(eB.getMessage contains "cancellation of all jobs")
      assert(sem.tryAcquire(1, 1, TimeUnit.MINUTES))
      assert(jobEnded.intValue == 3)
    } finally {
      threadPool.shutdownNow()
    }
  }

  test("Tags are isolated in multithreaded environment") {
    // Custom thread pool for multi-threaded testing
    val threadPool = Executors.newFixedThreadPool(2)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

    val session = SparkSession.builder().master("local").getOrCreate()
    @volatile var output1: Set[String] = null
    @volatile var output2: Set[String] = null

    def tag1(): Unit = {
      session.addTag("tag1")
      output1 = session.getTags()
    }

    def tag2(): Unit = {
      session.addTag("tag2")
      output2 = session.getTags()
    }

    try {
      // Run tasks in separate threads
      val future1 = Future {
        tag1()
      }
      val future2 = Future {
        tag2()
      }

      // Wait for threads to complete
      ThreadUtils.awaitResult(Future.sequence(Seq(future1, future2)), 1.minute)

      // Assert outputs
      assert(output1 != null)
      assert(output1 == Set("tag1"))
      assert(output2 != null)
      assert(output2 == Set("tag2"))
    } finally {
      threadPool.shutdownNow()
    }
  }
}
