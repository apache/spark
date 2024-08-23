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

import java.util.concurrent.{Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.internal.config.EXECUTOR_ALLOW_SPARK_CONTEXT
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.util.ExecutionListenerBus
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.ThreadUtils

/**
 * Test cases for the cancellation APIs provided by [[SparkSession]].
 */
@ExtendedSQLTest
class SparkSessionJobCancellationSuite
  extends SparkFunSuite
  with Eventually
  with LocalSparkContext {

  override def afterEach(): Unit = {
    try {
      // This suite should not interfere with the other test suites.
      SparkSession.getActiveSession.foreach(_.stop())
      SparkSession.clearActiveSession()
      SparkSession.getDefaultSession.foreach(_.stop())
      SparkSession.clearDefaultSession()
      resetSparkContext()
    } finally {
      super.afterEach()
    }
  }

  test("Cancellation APIs in SparkSession are isolated") {
    sc = new SparkContext("local[2]", "test")
    val globalSession = SparkSession.builder().sparkContext(sc).getOrCreate()
    var (sessionA, sessionB, sessionC): (SparkSession, SparkSession, SparkSession) =
      (null, null, null)

    // global ExecutionContext has only 2 threads in Apache Spark CI
    // create own thread pool for four Futures used in this test
    val numThreads = 3
    val fpool = ThreadUtils.newForkJoinPool("job-tags-test-thread-pool", numThreads)
    val executionContext = ExecutionContext.fromExecutorService(fpool)

    try {
      // Add a listener to release the semaphore once jobs are launched.
      val sem = new Semaphore(0)
      val jobEnded = new AtomicInteger(0)

      sc.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          sem.release()
        }

        override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
          sem.release()
          jobEnded.incrementAndGet()
        }
      })

      // Note: since tags are added in the Future threads, they don't need to be cleared in between.
      val jobA = Future {
        sessionA = globalSession.cloneSession()
        import globalSession.implicits._

        assert(sessionA.getTags() == Set())
        sessionA.addTag("two")
        assert(sessionA.getTags() == Set("two"))
        sessionA.clearTags() // check that clearing all tags works
        assert(sessionA.getTags() == Set())
        sessionA.addTag("one")
        assert(sessionA.getTags() == Set("one"))
        try {
          sessionA.range(1, 10000).map { i => Thread.sleep(100); i }.count()
        } finally {
          sessionA.clearTags() // clear for the case of thread reuse by another Future
        }
      }(executionContext)
      val jobB = Future {
        sessionB = globalSession.cloneSession()
        import globalSession.implicits._

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
      }(executionContext)
      val jobC = Future {
        sessionC = globalSession.cloneSession()
        import globalSession.implicits._

        sessionC.addTag("foo")
        sessionC.removeTag("foo")
        assert(sessionC.getTags() == Set()) // check that remove works removing the last tag
        sessionC.addTag("boo")
        try {
          sessionC.range(1, 10000, 2).map { i => Thread.sleep(100); i }.count()
        } finally {
          sessionC.clearTags() // clear for the case of thread reuse by another Future
        }
      }(executionContext)

      // Block until four jobs have started.
      assert(sem.tryAcquire(3, 1, TimeUnit.MINUTES))

      // Tags are applied
      val threeJobs = sc.dagScheduler.activeJobs
      assert(threeJobs.size == 3)
      for(ss <- Seq(sessionA, sessionB, sessionC)) {
        val job = threeJobs.filter(_.getSparkSessionUUID.getOrElse("") == ss.sessionUUID)
        assert(job.size == 1)
        val tags = job.head.properties.get(SparkContext.SPARK_JOB_TAGS).asInstanceOf[String]
          .split(SparkContext.SPARK_JOB_TAGS_SEP)
        assert(tags.forall(_.contains(s"spark-session-${ss.sessionUUID}-")))
        val userTags = tags.map(_.replace(s"spark-session-${ss.sessionUUID}-", ""))
        ss match {
          case s if s == sessionA => assert(userTags.toSet == Set("one"))
          case s if s == sessionB => assert(userTags.toSet == Set("one", "two"))
          case s if s == sessionC => assert(userTags.toSet == Set("boo"))
        }
      }

      // Global session cancels nothing
      assert(globalSession.interruptAll().isEmpty)
      assert(globalSession.interruptTag("one").isEmpty)
      assert(globalSession.interruptTag("two").isEmpty)
      for (i <- 0 until globalSession.sparkContext.dagScheduler.numTotalJobs) {
        assert(globalSession.interruptOperation(i.toString).isEmpty)
      }
      assert(jobEnded.intValue == 0)

      // One job cancelled
      for (i <- 0 until globalSession.sparkContext.dagScheduler.numTotalJobs) {
        sessionC.interruptOperation(i.toString)
      }
      val eC = intercept[SparkException] {
        ThreadUtils.awaitResult(jobC, 1.minute)
      }.getCause
      assert(eC.getMessage contains "Interrupted")
      assert(sem.tryAcquire(1, 1, TimeUnit.MINUTES))
      assert(jobEnded.intValue == 1)

      // Another job cancelled
      assert(sessionA.interruptTag("one").size == 1)
      val eA = intercept[SparkException] {
        ThreadUtils.awaitResult(jobA, 1.minute)
      }.getCause
      assert(eA.getMessage contains "Interrupted")
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
      fpool.shutdownNow()
    }
  }


  test("SPARK-34087: Fix memory leak of ExecutionListenerBus") {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    @inline def listenersNum(): Int = {
      spark.sparkContext
        .listenerBus
        .listeners
        .asScala
        .count(_.isInstanceOf[ExecutionListenerBus])
    }

    (1 to 10).foreach { _ =>
      spark.cloneSession()
      SparkSession.clearActiveSession()
    }

    eventually(timeout(10.seconds), interval(1.seconds)) {
      System.gc()
      // After GC, the number of ExecutionListenerBus should be less than 11 (we created 11
      // SparkSessions in total).
      // Since GC can't 100% guarantee all out-of-referenced objects be cleaned at one time,
      // here, we check at least one listener is cleaned up to prove the mechanism works.
      assert(listenersNum() < 11)
    }
  }

  test("create with config options and propagate them to SparkContext and SparkSession") {
    val session = SparkSession.builder()
      .master("local")
      .config(UI_ENABLED.key, value = false)
      .config("some-config", "v2")
      .getOrCreate()
    assert(session.sparkContext.conf.get("some-config") == "v2")
    assert(session.conf.get("some-config") == "v2")
  }

  test("use global default session") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.builder().getOrCreate() == session)
  }

  test("sets default and active session") {
    assert(SparkSession.getDefaultSession == None)
    assert(SparkSession.getActiveSession == None)
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.getDefaultSession == Some(session))
    assert(SparkSession.getActiveSession == Some(session))
  }

  test("get active or default session") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(SparkSession.active == session)
    SparkSession.clearActiveSession()
    assert(SparkSession.active == session)
    SparkSession.clearDefaultSession()
    intercept[SparkException](SparkSession.active)
    session.stop()
  }

  test("config options are propagated to existing SparkSession") {
    val session1 = SparkSession.builder().master("local").config("spark-config1", "a").getOrCreate()
    assert(session1.conf.get("spark-config1") == "a")
    val session2 = SparkSession.builder().config("spark-config1", "b").getOrCreate()
    assert(session1 == session2)
    assert(session1.conf.get("spark-config1") == "b")
  }

  test("use session from active thread session and propagate config options") {
    val defaultSession = SparkSession.builder().master("local").getOrCreate()
    val activeSession = defaultSession.newSession()
    SparkSession.setActiveSession(activeSession)
    val session = SparkSession.builder().config("spark-config2", "a").getOrCreate()

    assert(activeSession != defaultSession)
    assert(session == activeSession)
    assert(session.conf.get("spark-config2") == "a")
    assert(session.sessionState.conf == SQLConf.get)
    assert(SQLConf.get.getConfString("spark-config2") == "a")
    SparkSession.clearActiveSession()

    assert(SparkSession.builder().getOrCreate() == defaultSession)
  }

  test("create a new session if the default session has been stopped") {
    val defaultSession = SparkSession.builder().master("local").getOrCreate()
    SparkSession.setDefaultSession(defaultSession)
    defaultSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != defaultSession)
  }

  test("create a new session if the active thread session has been stopped") {
    val activeSession = SparkSession.builder().master("local").getOrCreate()
    SparkSession.setActiveSession(activeSession)
    activeSession.stop()
    val newSession = SparkSession.builder().master("local").getOrCreate()
    assert(newSession != activeSession)
  }

  test("create SparkContext first then SparkSession") {
    val conf = new SparkConf().setAppName("test").setMaster("local").set("key1", "value1")
    val sparkContext2 = new SparkContext(conf)
    val session = SparkSession.builder().config("key2", "value2").getOrCreate()
    assert(session.conf.get("key1") == "value1")
    assert(session.conf.get("key2") == "value2")
    assert(session.sparkContext == sparkContext2)
    // We won't update conf for existing `SparkContext`
    assert(!sparkContext2.conf.contains("key2"))
    assert(sparkContext2.conf.get("key1") == "value1")
  }

  test("create SparkContext first then pass context to SparkSession") {
    val conf = new SparkConf().setAppName("test").setMaster("local").set("key1", "value1")
    val newSC = new SparkContext(conf)
    val session = SparkSession.builder().sparkContext(newSC).config("key2", "value2").getOrCreate()
    assert(session.conf.get("key1") == "value1")
    assert(session.conf.get("key2") == "value2")
    assert(session.sparkContext == newSC)
    assert(session.sparkContext.conf.get("key1") == "value1")
    // If the created sparkContext is passed through the Builder's API sparkContext,
    // the conf of this sparkContext will not contain the conf set through the API config.
    assert(!session.sparkContext.conf.contains("key2"))
    assert(session.sparkContext.conf.get("spark.app.name") == "test")
  }

  test("SPARK-15887: hive-site.xml should be loaded") {
    val session = SparkSession.builder().master("local").getOrCreate()
    assert(session.sessionState.newHadoopConf().get("hive.in.test") == "true")
    assert(session.sparkContext.hadoopConfiguration.get("hive.in.test") == "true")
  }

  test("SPARK-15991: Set global Hadoop conf") {
    val session = SparkSession.builder().master("local").getOrCreate()
    val mySpecialKey = "my.special.key.15991"
    val mySpecialValue = "msv"
    try {
      session.sparkContext.hadoopConfiguration.set(mySpecialKey, mySpecialValue)
      assert(session.sessionState.newHadoopConf().get(mySpecialKey) == mySpecialValue)
    } finally {
      session.sparkContext.hadoopConfiguration.unset(mySpecialKey)
    }
  }

  test("SPARK-31234: RESET command will not change static sql configs and " +
    "spark context conf values in SessionState") {
    val session = SparkSession.builder()
      .master("local")
      .config(GLOBAL_TEMP_DATABASE.key, value = "globalTempDB-SPARK-31234")
      .config("spark.app.name", "test-app-SPARK-31234")
      .getOrCreate()

    assert(session.conf.get("spark.app.name") === "test-app-SPARK-31234")
    assert(session.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31234")
    session.sql("RESET")
    assert(session.conf.get("spark.app.name") === "test-app-SPARK-31234")
    assert(session.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31234")
  }

  test("SPARK-31354: SparkContext only register one SparkSession ApplicationEnd listener") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-app-SPARK-31354-1")
    val context = new SparkContext(conf)
    SparkSession
      .builder()
      .sparkContext(context)
      .master("local")
      .getOrCreate()
    val postFirstCreation = context.listenerBus.listeners.size()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    SparkSession
      .builder()
      .sparkContext(context)
      .master("local")
      .getOrCreate()
    val postSecondCreation = context.listenerBus.listeners.size()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    assert(postFirstCreation == postSecondCreation)
  }

  test("SPARK-31532: should not propagate static sql configs to the existing" +
    " active/default SparkSession") {
    val session = SparkSession.builder()
      .master("local")
      .config(GLOBAL_TEMP_DATABASE.key, value = "globalTempDB-SPARK-31532")
      .config("spark.app.name", "test-app-SPARK-31532")
      .getOrCreate()
    // do not propagate static sql configs to the existing active session
    val session1 = SparkSession
      .builder()
      .config(GLOBAL_TEMP_DATABASE.key, "globalTempDB-SPARK-31532-1")
      .getOrCreate()
    assert(session.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532")
    assert(session1.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532")

    // do not propagate static sql configs to the existing default session
    SparkSession.clearActiveSession()
    val session2 = SparkSession
      .builder()
      .config(WAREHOUSE_PATH.key, "SPARK-31532-db")
      .config(GLOBAL_TEMP_DATABASE.key, value = "globalTempDB-SPARK-31532-2")
      .getOrCreate()

    assert(!session.conf.get(WAREHOUSE_PATH).contains("SPARK-31532-db"))
    assert(session.conf.get(WAREHOUSE_PATH) === session2.conf.get(WAREHOUSE_PATH))
    assert(session2.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532")
  }

  test("SPARK-31532: propagate static sql configs if no existing SparkSession") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-app-SPARK-31532-2")
      .set(GLOBAL_TEMP_DATABASE.key, "globaltempdb-spark-31532")
      .set(WAREHOUSE_PATH.key, "SPARK-31532-db")
    SparkContext.getOrCreate(conf)

    // propagate static sql configs if no existing session
    val session = SparkSession
      .builder()
      .config(GLOBAL_TEMP_DATABASE.key, "globalTempDB-SPARK-31532-2")
      .config(WAREHOUSE_PATH.key, "SPARK-31532-db-2")
      .getOrCreate()
    assert(session.conf.get("spark.app.name") === "test-app-SPARK-31532-2")
    assert(session.conf.get(GLOBAL_TEMP_DATABASE) === "globaltempdb-spark-31532-2")
    assert(session.conf.get(WAREHOUSE_PATH) contains "SPARK-31532-db-2")
  }

  test("SPARK-32062: reset listenerRegistered in SparkSession") {
    (1 to 2).foreach { i =>
      val conf = new SparkConf()
        .setMaster("local")
        .setAppName(s"test-SPARK-32062-$i")
      val context = new SparkContext(conf)
      val beforeListenerSize = context.listenerBus.listeners.size()
      SparkSession
        .builder()
        .sparkContext(context)
        .getOrCreate()
      val afterListenerSize = context.listenerBus.listeners.size()
      assert(beforeListenerSize + 1 == afterListenerSize)
      context.stop()
    }
  }

  test("SPARK-32160: Disallow to create SparkSession in executors") {
    val session = SparkSession.builder().master("local-cluster[3, 1, 1024]").getOrCreate()

    val error = intercept[SparkException] {
      session.range(1).foreach { v =>
        SparkSession.builder().master("local").getOrCreate()
        ()
      }
    }.getMessage()

    assert(error.contains("SparkSession should only be created and accessed on the driver."))
  }

  test("SPARK-32160: Allow to create SparkSession in executors if the config is set") {
    val session = SparkSession.builder().master("local-cluster[3, 1, 1024]").getOrCreate()

    session.range(1).foreach { v =>
      SparkSession.builder().master("local")
        .config(EXECUTOR_ALLOW_SPARK_CONTEXT.key, true).getOrCreate().stop()
      ()
    }
  }

  test("SPARK-32991: Use conf in shared state as the original configuration for RESET") {
    val wh = "spark.sql.warehouse.dir"
    val td = "spark.sql.globalTempDatabase"
    val custom = "spark.sql.custom"

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SPARK-32991")
      .set(wh, "./data1")
      .set(td, "bob")

    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .config(wh, "./data2")
      .config(td, "alice")
      .config(custom, "kyao")
      .getOrCreate()

    // When creating the first session like above, we will update the shared spark conf to the
    // newly specified values
    val sharedWH = spark.sharedState.conf.get(wh)
    val sharedTD = spark.sharedState.conf.get(td)
    assert(sharedWH contains "data2",
      "The warehouse dir in shared state should be determined by the 1st created spark session")
    assert(sharedTD === "alice",
      "Static sql configs in shared state should be determined by the 1st created spark session")
    assert(spark.sharedState.conf.getOption(custom).isEmpty,
      "Dynamic sql configs is session specific")

    assert(spark.conf.get(wh) contains sharedWH,
      "The warehouse dir in session conf and shared state conf should be consistent")
    assert(spark.conf.get(td) === sharedTD,
      "Static sql configs in session conf and shared state conf should be consistent")
    assert(spark.conf.get(custom) === "kyao", "Dynamic sql configs is session specific")

    spark.sql("RESET")

    assert(spark.conf.get(wh) contains sharedWH,
      "The warehouse dir in shared state should be respect after RESET")
    assert(spark.conf.get(td) === sharedTD,
      "Static sql configs in shared state should be respect after RESET")
    assert(spark.conf.get(custom) === "kyao",
      "Dynamic sql configs in session initial map should be respect after RESET")

    val spark2 = SparkSession.builder()
      .config(wh, "./data3")
      .config(custom, "kyaoo").getOrCreate()
    assert(spark2.conf.get(wh) contains sharedWH)
    assert(spark2.conf.get(td) === sharedTD)
    assert(spark2.conf.get(custom) === "kyaoo")
  }

  test("SPARK-32991: RESET should work properly with multi threads") {
    val wh = "spark.sql.warehouse.dir"
    val td = "spark.sql.globalTempDatabase"
    val custom = "spark.sql.custom"
    val spark = ThreadUtils.runInNewThread("new session 0", false) {
      SparkSession.builder()
        .master("local")
        .config(wh, "./data0")
        .config(td, "bob")
        .config(custom, "c0")
        .getOrCreate()
    }

    spark.sql(s"SET $custom=c1")
    assert(spark.conf.get(custom) === "c1")
    spark.sql("RESET")
    assert(spark.conf.get(wh) contains "data0",
      "The warehouse dir in shared state should be respect after RESET")
    assert(spark.conf.get(td) === "bob",
      "Static sql configs in shared state should be respect after RESET")
    assert(spark.conf.get(custom) === "c0",
      "Dynamic sql configs in shared state should be respect after RESET")

    val spark1 = ThreadUtils.runInNewThread("new session 1", false) {
      SparkSession.builder().getOrCreate()
    }

    assert(spark === spark1)

    // TODO: SPARK-33718: After clear sessions, the SharedState will be unreachable, then all
    // the new static will take effect.
    SparkSession.clearDefaultSession()
    val spark2 = ThreadUtils.runInNewThread("new session 2", false) {
      SparkSession.builder()
        .master("local")
        .config(wh, "./data1")
        .config(td, "alice")
        .config(custom, "c2")
        .getOrCreate()
    }

    assert(spark2 !== spark)
    spark2.sql(s"SET $custom=c1")
    assert(spark2.conf.get(custom) === "c1")
    spark2.sql("RESET")
    assert(spark2.conf.get(wh) contains "data1")
    assert(spark2.conf.get(td) === "alice")
    assert(spark2.conf.get(custom) === "c2")

  }

  test("SPARK-33944: warning setting hive.metastore.warehouse.dir using session options") {
    val msg = "Not allowing to set hive.metastore.warehouse.dir in SparkSession's options"
    val logAppender = new LogAppender(msg)
    withLogAppender(logAppender) {
      SparkSession.builder()
        .master("local")
        .config("hive.metastore.warehouse.dir", "any")
        .getOrCreate()
        .sharedState
    }
    assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(msg)))
  }

  test("SPARK-33944: no warning setting spark.sql.warehouse.dir using session options") {
    val msg = "Not allowing to set hive.metastore.warehouse.dir in SparkSession's options"
    val logAppender = new LogAppender(msg)
    withLogAppender(logAppender) {
      SparkSession.builder()
        .master("local")
        .config("spark.sql.warehouse.dir", "any")
        .getOrCreate()
        .sharedState
    }
    assert(!logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(msg)))
  }

  Seq(".", "..", "dir0", "dir0/dir1", "/dir0/dir1", "./dir0").foreach { pathStr =>
    test(s"SPARK-34558: warehouse path ($pathStr) should be qualified for spark/hadoop conf") {
      val path = new Path(pathStr)
      val conf = new SparkConf().set(WAREHOUSE_PATH, pathStr)
      val session = SparkSession.builder()
        .master("local")
        .config(conf)
        .getOrCreate()
      val hadoopConf = session.sessionState.newHadoopConf()
      val expected = path.getFileSystem(hadoopConf).makeQualified(path).toString
      // session related configs
      assert(hadoopConf.get("hive.metastore.warehouse.dir") === expected)
      assert(session.conf.get(WAREHOUSE_PATH) === expected)
      assert(session.sessionState.conf.warehousePath === expected)

      // shared configs
      assert(session.sharedState.conf.get(WAREHOUSE_PATH) === expected)
      assert(session.sharedState.hadoopConf.get("hive.metastore.warehouse.dir") === expected)

      // spark context configs
      assert(session.sparkContext.conf.get(WAREHOUSE_PATH) === expected)
      assert(session.sparkContext.hadoopConfiguration.get("hive.metastore.warehouse.dir") ===
        expected)
    }
  }

  test("SPARK-34558: Create a working SparkSession with a broken FileSystem") {
    val msg = "Cannot qualify the warehouse path, leaving it unqualified"
    val logAppender = new LogAppender(msg)
    withLogAppender(logAppender) {
      val session =
        SparkSession.builder()
          .master("local")
          .config(WAREHOUSE_PATH.key, "unknown:///mydir")
          .getOrCreate()
      session.sql("SELECT 1").collect()
    }
    assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(msg)))
  }

  test("SPARK-37727: Show ignored configurations in debug level logs") {
    // Create one existing SparkSession to check following logs.
    SparkSession.builder().master("local").getOrCreate()

    val logAppender = new LogAppender
    logAppender.setThreshold(Level.DEBUG)
    withLogAppender(logAppender, level = Some(Level.DEBUG)) {
      SparkSession.builder()
        .config("spark.sql.warehouse.dir", "2")
        .config("spark.abc", "abcb")
        .config("spark.abcd", "abcb4")
        .getOrCreate()
    }

    val logs = logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
    Seq(
        "Ignored static SQL configurations",
        "spark.sql.warehouse.dir=2",
        "Configurations that might not take effect",
        "spark.abcd=abcb4",
        "spark.abc=abcb").foreach { msg =>
      assert(logs.exists(_.contains(msg)), s"$msg did not exist in:\n${logs.mkString("\n")}")
    }
  }

  test("SPARK-37727: Hide the same configuration already explicitly set in logs") {
    // Create one existing SparkSession to check following logs.
    SparkSession.builder().master("local").config("spark.abc", "abc").getOrCreate()

    val logAppender = new LogAppender
    logAppender.setThreshold(Level.DEBUG)
    withLogAppender(logAppender, level = Some(Level.DEBUG)) {
      // Ignore logs because it's already set.
      SparkSession.builder().config("spark.abc", "abc").getOrCreate()
      // Show logs for only configuration newly set.
      SparkSession.builder().config("spark.abc.new", "abc").getOrCreate()
      // Ignore logs because it's set ^.
      SparkSession.builder().config("spark.abc.new", "abc").getOrCreate()
    }

    val logs = logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
    Seq(
      "Using an existing Spark session; only runtime SQL configurations will take effect",
      "Configurations that might not take effect",
      "spark.abc.new=abc").foreach { msg =>
      assert(logs.exists(_.contains(msg)), s"$msg did not exist in:\n${logs.mkString("\n")}")
    }

    assert(
      !logs.exists(_.contains("spark.abc=abc")),
      s"'spark.abc=abc' existed in:\n${logs.mkString("\n")}")
  }

  test("SPARK-37727: Hide runtime SQL configurations in logs") {
    // Create one existing SparkSession to check following logs.
    SparkSession.builder().master("local").getOrCreate()

    val logAppender = new LogAppender
    logAppender.setThreshold(Level.DEBUG)
    withLogAppender(logAppender, level = Some(Level.DEBUG)) {
      // Ignore logs for runtime SQL configurations
      SparkSession.builder().config("spark.sql.ansi.enabled", "true").getOrCreate()
      // Show logs for Spark core configuration
      SparkSession.builder().config("spark.buffer.size", "1234").getOrCreate()
      // Show logs for custom runtime options
      SparkSession.builder().config("spark.sql.source.abc", "abc").getOrCreate()
      // Show logs for static SQL configurations
      SparkSession.builder().config("spark.sql.warehouse.dir", "xyz").getOrCreate()
    }

    val logs = logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
    Seq(
      "spark.buffer.size=1234",
      "spark.sql.source.abc=abc",
      "spark.sql.warehouse.dir=xyz").foreach { msg =>
      assert(logs.exists(_.contains(msg)), s"$msg did not exist in:\n${logs.mkString("\n")}")
    }

    assert(
      !logs.exists(_.contains("spark.sql.ansi.enabled\"")),
      s"'spark.sql.ansi.enabled' existed in:\n${logs.mkString("\n")}")
  }

  test("SPARK-40163: SparkSession.config(Map)") {
    val map: Map[String, Any] = Map(
      "string" -> "",
      "boolean" -> true,
      "double" -> 0.0,
      "long" -> 0L
    )

    val session = SparkSession.builder()
      .master("local")
      .config(map)
      .getOrCreate()

    for (e <- map) {
      assert(session.conf.get(e._1) == e._2.toString)
    }
  }
}
