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

package org.apache.spark.sql.connect.service

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{TimeoutException, TimeUnit}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.sys.process.Process
import scala.util.Random
import scala.util.control.NonFatal

import com.google.common.collect.Lists
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkEnv
import org.apache.spark.api.python.SimplePythonFunction
import org.apache.spark.connect.proto
import org.apache.spark.sql.IntegratedUDFTestUtils
import org.apache.spark.sql.connect.{PythonTestDepsChecker, SparkConnectTestUtils}
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.planner.{PythonStreamingQueryListener, SparkConnectPlanner, StreamingForeachBatchHelper}
import org.apache.spark.sql.connect.planner.StreamingForeachBatchHelper.RunnerCleaner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.graph.{DataflowGraph, PipelineUpdateContextImpl}
import org.apache.spark.sql.pipelines.logging.PipelineEvent
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ArrayImplicits._

class SparkConnectSessionHolderSuite extends SharedSparkSession {

  test("DataFrame cache: Successful put and get") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    import sessionHolder.session.implicits._

    val data1 = Seq(("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
    val df1 = data1.toDF()
    val id1 = "df_id_1"
    sessionHolder.cacheDataFrameById(id1, df1)

    val expectedDf1 = sessionHolder.getDataFrameOrThrow(id1)
    assert(expectedDf1 == df1)

    val data2 = Seq(("k4", "v4"), ("k5", "v5"))
    val df2 = data2.toDF()
    val id2 = "df_id_2"
    sessionHolder.cacheDataFrameById(id2, df2)

    val expectedDf2 = sessionHolder.getDataFrameOrThrow(id2)
    assert(expectedDf2 == df2)
  }

  test("DataFrame cache: Should throw when dataframe is not found") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    import sessionHolder.session.implicits._

    val key1 = "key_1"

    assertThrows[InvalidPlanInput] {
      sessionHolder.getDataFrameOrThrow(key1)
    }

    val data1 = Seq(("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
    val df1 = data1.toDF()
    sessionHolder.cacheDataFrameById(key1, df1)
    sessionHolder.getDataFrameOrThrow(key1)

    val key2 = "key_2"
    assertThrows[InvalidPlanInput] {
      sessionHolder.getDataFrameOrThrow(key2)
    }
  }

  test("DataFrame cache: Remove cache and then get should fail") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    import sessionHolder.session.implicits._

    val key1 = "key_1"
    val data1 = Seq(("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
    val df1 = data1.toDF()
    sessionHolder.cacheDataFrameById(key1, df1)
    sessionHolder.getDataFrameOrThrow(key1)

    sessionHolder.removeCachedDataFrame(key1)
    assertThrows[InvalidPlanInput] {
      sessionHolder.getDataFrameOrThrow(key1)
    }
  }

  private def streamingForeachBatchFunction(pysparkPythonPath: String): Array[Byte] = {
    var binaryFunc: Array[Byte] = null
    withTempPath { path =>
      Process(
        Seq(
          IntegratedUDFTestUtils.pythonExec,
          "-c",
          "from pyspark.serializers import CloudPickleSerializer; " +
            s"f = open('$path', 'wb');" +
            "f.write(CloudPickleSerializer().dumps((" +
            "lambda df, batchId: batchId)))"),
        None,
        "PYTHONPATH" -> pysparkPythonPath).!!
      binaryFunc = Files.readAllBytes(path.toPath)
    }
    assert(binaryFunc != null)
    binaryFunc
  }

  private def streamingQueryListenerFunction(pysparkPythonPath: String): Array[Byte] = {
    var binaryFunc: Array[Byte] = null
    val pythonScript =
      """
        |from pyspark.sql.streaming.listener import StreamingQueryListener
        |
        |class MyListener(StreamingQueryListener):
        |    def onQueryStarted(e):
        |        pass
        |
        |    def onQueryIdle(e):
        |        pass
        |
        |    def onQueryProgress(e):
        |        pass
        |
        |    def onQueryTerminated(e):
        |        pass
        |
        |listener = MyListener()
      """.stripMargin
    withTempPath { codePath =>
      Files.write(codePath.toPath, pythonScript.getBytes(StandardCharsets.UTF_8))
      withTempPath { path =>
        Process(
          Seq(
            IntegratedUDFTestUtils.pythonExec,
            "-c",
            "from pyspark.serializers import CloudPickleSerializer; " +
              s"f = open('$path', 'wb');" +
              s"exec(open('$codePath', 'r').read());" +
              "f.write(CloudPickleSerializer().dumps(listener))"),
          None,
          "PYTHONPATH" -> pysparkPythonPath).!!
        binaryFunc = Files.readAllBytes(path.toPath)
      }
    }
    assert(binaryFunc != null)
    binaryFunc
  }

  private def dummyPythonFunction(sessionHolder: SessionHolder)(
      fcn: String => Array[Byte]): SimplePythonFunction = {
    val sparkPythonPath =
      s"${IntegratedUDFTestUtils.pysparkPythonPath}:${IntegratedUDFTestUtils.pythonPath}"

    SimplePythonFunction(
      command = fcn(sparkPythonPath).toImmutableArraySeq,
      envVars = mutable.Map("PYTHONPATH" -> sparkPythonPath).asJava,
      pythonIncludes = sessionHolder.artifactManager.getPythonIncludes.asJava,
      pythonExec = IntegratedUDFTestUtils.pythonExec,
      pythonVer = IntegratedUDFTestUtils.pythonVer,
      broadcastVars = Lists.newArrayList(),
      accumulator = null)
  }

  test("python listener process: process terminates after listener is removed") {
    // scalastyle:off assume
    assume(IntegratedUDFTestUtils.shouldTestPandasUDFs)
    assume(PythonTestDepsChecker.isConnectDepsAvailable)
    // scalastyle:on assume

    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    try {
      SparkConnectService.start(spark.sparkContext)

      val pythonFn = dummyPythonFunction(sessionHolder)(streamingQueryListenerFunction)

      val id1 = "listener_removeListener_test_1"
      val id2 = "listener_removeListener_test_2"
      val listener1 = new PythonStreamingQueryListener(pythonFn, sessionHolder)
      val listener2 = new PythonStreamingQueryListener(pythonFn, sessionHolder)

      sessionHolder.cacheListenerById(id1, listener1)
      spark.streams.addListener(listener1)
      sessionHolder.cacheListenerById(id2, listener2)
      spark.streams.addListener(listener2)

      val (runner1, runner2) = (listener1.runner, listener2.runner)

      // assert both python processes are running
      assert(!runner1.isWorkerStopped().get)
      assert(!runner2.isWorkerStopped().get)

      // remove listener1
      spark.streams.removeListener(listener1)
      sessionHolder.removeCachedListener(id1)
      // assert listener1's python process is not running
      eventually(timeout(30.seconds)) {
        assert(runner1.isWorkerStopped().get)
        assert(!runner2.isWorkerStopped().get)
      }

      // remove listener2
      spark.streams.removeListener(listener2)
      sessionHolder.removeCachedListener(id2)
      eventually(timeout(30.seconds)) {
        // assert listener2's python process is not running
        assert(runner2.isWorkerStopped().get)
        // all listeners are removed
        assert(spark.streams.listListeners().isEmpty)
      }
    } finally {
      SparkConnectService.stop()
    }
  }

  // Log and swallow best-effort cleanup failures so they do not mask a primary test
  // failure. InterruptedException re-asserts the interrupt flag on the current thread;
  // fatal errors (OOM, StackOverflow, LinkageError) propagate.
  private def runQuietly(label: String, op: => Unit): Unit = {
    try op
    catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
      case NonFatal(t) =>
        // scalastyle:off println
        println(s"===== $label suppressed ${t.getClass.getSimpleName}: ${t.getMessage} =====")
      // scalastyle:on println
    }
  }

  // Same semantics as SparkFunSuite.retry, but prints to stdout so retries show up in the
  // GitHub Actions job log (SparkFunSuite.retry's log4j output only lands in
  // target/unit-tests.log, surfaced as an artifact rather than in the live log).
  private def retryWithVisibleLog(maxAttempts: Int)(body: => Unit): Unit = {
    var attempt = 1
    var done = false
    while (!done) {
      try {
        body
        done = true
      } catch {
        case NonFatal(t) if attempt >= maxAttempts => throw t
        case NonFatal(t) =>
          // scalastyle:off println
          println(
            s"===== Attempt $attempt/$maxAttempts failed " +
              s"(${t.getClass.getSimpleName}: ${t.getMessage}); retrying =====")
          // scalastyle:on println
          // A leaked worker from this attempt may still hold sockets/listeners; do not
          // let afterEach/beforeEach throwing on that residual state abort the retry loop.
          runQuietly("afterEach", afterEach())
          runQuietly("beforeEach", beforeEach())
          attempt += 1
      }
    }
  }

  private def awaitTestBodyInNewThread(timeoutMillis: Long, onTimeout: () => Unit)(
      body: => Unit): Unit = {
    @volatile var error: Throwable = null
    val runnable: Runnable = () => {
      try {
        body
      } catch {
        case t: Throwable => error = t
      }
    }
    val worker = new Thread(runnable, s"${getClass.getSimpleName}-testBody-worker")
    worker.setDaemon(true)
    worker.start()
    worker.join(timeoutMillis)
    if (worker.isAlive) {
      // Capture the worker's stack so post-mortem diagnostics can identify which leaked
      // thread belongs to which attempt without a separate jstack.
      // scalastyle:off println
      println(
        s"===== Test body did not complete within $timeoutMillis ms " +
          s"(thread=${worker.getName}, state=${worker.getState}); stack trace follows =====")
      worker.getStackTrace.foreach(frame => println(s"  at $frame"))
      // scalastyle:on println
      // Best-effort: release any resource the worker is blocked on so it can unwind its own
      // finally and stop holding global state (SparkConnectService, listeners, ...).
      onTimeout()
      // Also interrupt the worker so any interruptible blocking call (e.g. the Thread.join
      // inside StreamExecution.interruptAndAwaitExecutionThreadTermination) wakes up.
      worker.interrupt()
      // Grace period for the now-unblocked worker to run its own finally
      // (SparkConnectService.stop() then the ~4s settle sleep).
      val gracePeriodMs = 30.seconds.toMillis
      worker.join(gracePeriodMs)
      val te = new TimeoutException(
        s"Test body did not complete within $timeoutMillis ms " +
          s"(after a $gracePeriodMs ms post-cleanup grace period)")
      // If the body finished during the grace window, surface the original failure
      // as the cause so a slow assertion failure is not misreported as a pure hang.
      if (!worker.isAlive && error != null) te.initCause(error)
      throw te
    }
    if (error != null) throw error
  }

  private def runPythonForeachBatchTerminationTestBody(sessionHolder: SessionHolder): Unit = {
    // Unique query names per attempt: a leaked query from a timed-out attempt may still
    // occupy the old name in spark.streams.active.
    val suffix = s"_${System.nanoTime()}"
    val q1Name = s"foreachBatch_termination_test_q1$suffix"
    val q2Name = s"foreachBatch_termination_test_q2$suffix"

    // Snapshot listeners before this attempt registers anything so we can scope cleanup and
    // assertions to listeners we added -- even if a previous timed-out attempt leaked a worker
    // whose own finally is racing with us.
    val baselineListeners = spark.streams.listListeners().toSet
    var capturedServer: AnyRef = null
    var ourNewListeners = Set.empty[StreamingQueryListener]

    try {
      // A previous timed-out attempt's leaked worker may still hold `started=true`, which
      // would make `start()` below a no-op and cause this attempt to share (and later
      // re-stop) the stale server. Force-stop first so `start()` creates a fresh instance;
      // the identity check in `finally` then distinguishes attempts.
      if (SparkConnectService.started) {
        runQuietly("stale SparkConnectService.stop()", SparkConnectService.stop())
      }
      SparkConnectService.start(spark.sparkContext)
      // Identity-check the server in `finally`: a previous attempt's leaked finally must
      // not tear down a service belonging to a later attempt.
      capturedServer = SparkConnectService.server

      val pythonFn = dummyPythonFunction(sessionHolder)(streamingForeachBatchFunction)
      val (fn1, cleaner1) =
        StreamingForeachBatchHelper.pythonForeachBatchWrapper(pythonFn, sessionHolder)
      val (fn2, cleaner2) =
        StreamingForeachBatchHelper.pythonForeachBatchWrapper(pythonFn, sessionHolder)

      val query1 = spark.readStream
        .format("rate")
        .load()
        .writeStream
        .format("memory")
        .queryName(q1Name)
        .foreachBatch(fn1)
        .start()

      val query2 = spark.readStream
        .format("rate")
        .load()
        .writeStream
        .format("memory")
        .queryName(q2Name)
        .foreachBatch(fn2)
        .start()

      sessionHolder.streamingForeachBatchRunnerCleanerCache
        .registerCleanerForQuery(query1, cleaner1)
      sessionHolder.streamingForeachBatchRunnerCleanerCache
        .registerCleanerForQuery(query2, cleaner2)

      // The first registerCleanerForQuery lazily registers the cleaner listener. Capture the
      // listeners we added so finally only removes ours, not a concurrent attempt's.
      ourNewListeners = spark.streams.listListeners().toSet -- baselineListeners

      val (runner1, runner2) =
        (cleaner1.asInstanceOf[RunnerCleaner].runner, cleaner2.asInstanceOf[RunnerCleaner].runner)

      // assert both python processes are running
      assert(!runner1.isWorkerStopped().get)
      assert(!runner2.isWorkerStopped().get)
      // stop query1
      query1.stop()
      // assert query1's python process is not running
      eventually(timeout(30.seconds)) {
        assert(runner1.isWorkerStopped().get)
        assert(!runner2.isWorkerStopped().get)
      }

      // stop query2
      query2.stop()
      eventually(timeout(30.seconds)) {
        // assert query2's python process is not running
        assert(runner2.isWorkerStopped().get)
      }

      // Only assert this attempt's queries stopped; a previous timed-out attempt may have
      // leaked queries into spark.streams.active that we cannot synchronously clean up.
      assert(!spark.streams.active.exists(q => q.name == q1Name || q.name == q2Name))
      // Scoped to this attempt: exactly one new listener (the cleaner listener) should
      // have been registered, regardless of any listeners leaked by a prior attempt.
      assert(
        ourNewListeners.size == 1,
        s"expected exactly 1 new listener registered by this attempt, " +
          s"got ${ourNewListeners.size}")
    } finally {
      // Only stop the service if it is still the one this attempt started; otherwise a
      // previous attempt's leaked finally would tear down the live service of the current
      // attempt.
      if (capturedServer != null && (SparkConnectService.server eq capturedServer)) {
        // Cleanup is best-effort: any failure must not mask the primary failure in the
        // try block, and the listener cleanup below must still run.
        runQuietly("SparkConnectService.stop()", SparkConnectService.stop())
        runQuietly("settle sleep", Thread.sleep(4.seconds.toMillis))
      }
      // Remove only the listeners this attempt registered; never touch a concurrent
      // attempt's process-termination listener. Wrapped in `runQuietly` so a throw here
      // cannot mask a primary failure in the try block.
      runQuietly("removeListeners", ourNewListeners.foreach(spark.streams.removeListener))
    }
  }

  test("python foreachBatch process: process terminates after query is stopped") {
    // scalastyle:off assume
    assume(IntegratedUDFTestUtils.shouldTestPandasUDFs)
    assume(PythonTestDepsChecker.isConnectDepsAvailable)
    // scalastyle:on assume

    // Bound query.stop() so it cannot hang indefinitely: spark.sql.streaming.stopTimeout
    // defaults to 0 (wait forever), which turns a stuck batch into an unkillable test.
    // 30s is small enough to fit under the outer per-attempt cap with room to spare.
    withSQLConf(SQLConf.STREAMING_STOP_TIMEOUT.key -> "30000") {
      retryWithVisibleLog(maxAttempts = 3) {
        // Run the body on a fresh daemon thread so the test thread can recover from a
        // hang in a non-interruptible socket read. SessionHolder is created outside the
        // body so onTimeout can close its Python worker sockets via cleanerCache; that
        // unblocks the hung dataIn.readInt so the leaked thread's finally can settle
        // before the next retry. 2-minute cap strictly bounds the original 150-minute hang.
        val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
        awaitTestBodyInNewThread(
          timeoutMillis = TimeUnit.MINUTES.toMillis(2),
          onTimeout = () =>
            runQuietly(
              "onTimeout cleanUpAll",
              sessionHolder.streamingForeachBatchRunnerCleanerCache.cleanUpAll())) {
          runPythonForeachBatchTerminationTestBody(sessionHolder)
        }
      }
    }
  }

  private def buildRelation(query: String) = {
    proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery(query)
          .build())
      .setCommon(proto.RelationCommon.newBuilder().setPlanId(Random.nextLong()).build())
      .build()
  }

  private def assertPlanCache(
      sessionHolder: SessionHolder,
      optionExpectedCachedRelations: Option[Set[proto.Relation]]) = {
    optionExpectedCachedRelations match {
      case Some(expectedCachedRelations) =>
        val cachedRelations = sessionHolder.getPlanCache.get.asMap().keySet().asScala
        assert(cachedRelations.size == expectedCachedRelations.size)
        expectedCachedRelations.foreach(relation => assert(cachedRelations.contains(relation)))
      case None => assert(sessionHolder.getPlanCache.isEmpty)
    }
  }

  test("Test session plan cache") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    try {
      // Set cache size to 2
      SparkEnv.get.conf.set(Connect.CONNECT_SESSION_PLAN_CACHE_SIZE, 2)
      val planner = new SparkConnectPlanner(sessionHolder)

      val random1 = buildRelation("select 1")
      val random2 = buildRelation("select 2")
      val random3 = buildRelation("select 3")
      val query1 = proto.Relation.newBuilder
        .setLimit(
          proto.Limit.newBuilder
            .setLimit(10)
            .setInput(
              proto.Relation
                .newBuilder()
                .setRange(proto.Range.newBuilder().setStart(0).setStep(1).setEnd(20))
                .build()))
        .setCommon(proto.RelationCommon.newBuilder().setPlanId(Random.nextLong()).build())
        .build()
      val query2 = proto.Relation.newBuilder
        .setLimit(proto.Limit.newBuilder.setLimit(5).setInput(query1))
        .setCommon(proto.RelationCommon.newBuilder().setPlanId(Random.nextLong()).build())
        .build()

      // If cachePlan is false, the cache is still empty.
      planner.transformRelation(random1, cachePlan = false)
      assertPlanCache(sessionHolder, Some(Set()))

      // Put a random entry in cache.
      planner.transformRelation(random1, cachePlan = true)
      assertPlanCache(sessionHolder, Some(Set(random1)))

      // Put another random entry in cache.
      planner.transformRelation(random2, cachePlan = true)
      assertPlanCache(sessionHolder, Some(Set(random1, random2)))

      // Analyze query1. We only cache the root relation, and the random1 is evicted.
      planner.transformRelation(query1, cachePlan = true)
      assertPlanCache(sessionHolder, Some(Set(random2, query1)))

      // Put another random entry in cache.
      planner.transformRelation(random3, cachePlan = true)
      assertPlanCache(sessionHolder, Some(Set(query1, random3)))

      // Analyze query2. As query1 is accessed during the process, it should be in the cache.
      planner.transformRelation(query2, cachePlan = true)
      assertPlanCache(sessionHolder, Some(Set(query1, query2)))
    } finally {
      // Set back to default value.
      SparkEnv.get.conf.set(Connect.CONNECT_SESSION_PLAN_CACHE_SIZE, 5)
    }
  }

  test("Test session plan cache - cache size zero or negative") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    try {
      // Set cache size to -1
      SparkEnv.get.conf.set(Connect.CONNECT_SESSION_PLAN_CACHE_SIZE, -1)
      val planner = new SparkConnectPlanner(sessionHolder)

      val query = buildRelation("select 1")

      // If cachePlan is false, the cache is still None.
      planner.transformRelation(query, cachePlan = false)
      assertPlanCache(sessionHolder, None)

      // Even if we specify "cachePlan = true", the cache is still None.
      planner.transformRelation(query, cachePlan = true)
      assertPlanCache(sessionHolder, None)
    } finally {
      // Set back to default value.
      SparkEnv.get.conf.set(Connect.CONNECT_SESSION_PLAN_CACHE_SIZE, 5)
    }
  }

  test("Test session plan cache - disabled") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    // Disable plan cache of the session
    try {
      sessionHolder.session.conf.set(Connect.CONNECT_SESSION_PLAN_CACHE_ENABLED.key, false)
      val planner = new SparkConnectPlanner(sessionHolder)

      val query = buildRelation("select 1")

      // If cachePlan is false, the cache is still empty.
      // Although the cache is created as cache size is greater than zero, it won't be used.
      planner.transformRelation(query, cachePlan = false)
      assertPlanCache(sessionHolder, Some(Set()))

      // Even if we specify "cachePlan = true", the cache is still empty.
      planner.transformRelation(query, cachePlan = true)
      assertPlanCache(sessionHolder, Some(Set()))
    } finally {
      sessionHolder.session.conf.set(Connect.CONNECT_SESSION_PLAN_CACHE_ENABLED, true)
    }
  }

  test("Test duplicate operation IDs") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.addOperationId("DUMMY")
    val ex = intercept[IllegalStateException] {
      sessionHolder.addOperationId("DUMMY")
    }
    assert(ex.getMessage.contains("already exists"))
  }

  test("getInactiveOperationInfo returns TerminationInfo for closed operations") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val operationId = executeHolder.operationId

    sessionHolder.closeOperation(executeHolder)

    val inactiveInfo = sessionHolder.getInactiveOperationInfo(operationId)
    assert(inactiveInfo.isDefined)
    assert(inactiveInfo.get.operationId == operationId)
  }

  test("getInactiveOperationInfo returns None for active operations") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val operationId = executeHolder.operationId

    assert(sessionHolder.getInactiveOperationInfo(operationId) == None)
  }

  test("getInactiveOperationInfo returns None for unknown operations") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    assert(sessionHolder.getInactiveOperationInfo("unknown-op") == None)
  }

  test("listInactiveOperations returns all closed operations") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    val command = proto.Command.newBuilder().build()
    val executeHolder1 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeHolder2 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeHolder3 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    sessionHolder.closeOperation(executeHolder1)
    sessionHolder.closeOperation(executeHolder2)
    sessionHolder.closeOperation(executeHolder3)

    val inactiveOps = sessionHolder.listInactiveOperations()
    assert(inactiveOps.size == 3)
    val inactiveOpIds = inactiveOps.map(_.operationId).toSet
    assert(inactiveOpIds.contains(executeHolder1.operationId))
    assert(inactiveOpIds.contains(executeHolder2.operationId))
    assert(inactiveOpIds.contains(executeHolder3.operationId))
  }

  test("listInactiveOperations returns empty for new session") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    assert(sessionHolder.listInactiveOperations().isEmpty)
  }

  test("listActiveOperationIds returns all active operations") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    val command = proto.Command.newBuilder().build()
    val executeHolder1 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeHolder2 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeHolder3 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val activeOps = sessionHolder.listActiveOperationIds()
    assert(activeOps.size == 3)
    val activeOpIds = activeOps.toSet
    assert(activeOpIds.contains(executeHolder1.operationId))
    assert(activeOpIds.contains(executeHolder2.operationId))
    assert(activeOpIds.contains(executeHolder3.operationId))
  }

  test("listActiveOperationIds returns empty for new session") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    assert(sessionHolder.listActiveOperationIds().isEmpty)
  }

  test("listActiveOperationIds excludes closed operations") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    val command = proto.Command.newBuilder().build()
    val executeHolder1 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeHolder2 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    sessionHolder.closeOperation(executeHolder1)

    val activeOps = sessionHolder.listActiveOperationIds()
    assert(activeOps.size == 1)
    assert(activeOps.contains(executeHolder2.operationId))
    assert(!activeOps.contains(executeHolder1.operationId))
  }

  test("Pipeline execution cache") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val graphId = "test_graph"
    val pipelineUpdateContext = new PipelineUpdateContextImpl(
      new DataflowGraph(Seq(), Seq(), Seq(), Seq()),
      (_: PipelineEvent) => None,
      storageRoot = "file:///test_storage_root")
    sessionHolder.cachePipelineExecution(graphId, pipelineUpdateContext)
    assert(
      sessionHolder.getPipelineExecution(graphId).nonEmpty,
      "pipeline execution was not cached")
    sessionHolder.removeAllPipelineExecutions()
    assert(
      sessionHolder.getPipelineExecution(graphId).isEmpty,
      "pipeline execution was not removed")
  }

  gridTest("Actively cache data source reads")(Seq(true, false)) { enabled =>
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val planner = new SparkConnectPlanner(sessionHolder)

    val dataSourceRead = proto.Relation
      .newBuilder()
      .setRead(
        proto.Read
          .newBuilder()
          .setDataSource(proto.Read.DataSource
            .newBuilder()
            .setSchema("col int")))
      .setCommon(proto.RelationCommon.newBuilder().setPlanId(Random.nextLong()).build())
      .build()
    val dataSourceReadJoin = proto.Relation
      .newBuilder()
      .setJoin(
        proto.Join
          .newBuilder()
          .setLeft(dataSourceRead)
          .setRight(dataSourceRead)
          .setJoinType(proto.Join.JoinType.JOIN_TYPE_CROSS))
      .setCommon(proto.RelationCommon.newBuilder().setPlanId(Random.nextLong()).build())
      .build()

    sessionHolder.session.conf
      .set(Connect.CONNECT_ALWAYS_CACHE_DATA_SOURCE_READS_ENABLED, enabled)
    planner.transformRelation(dataSourceReadJoin, cachePlan = true)
    val expected = if (enabled) {
      Set(dataSourceReadJoin, dataSourceRead)
    } else {
      Set(dataSourceReadJoin)
    }
    assertPlanCache(sessionHolder, Some(expected))
  }
}
