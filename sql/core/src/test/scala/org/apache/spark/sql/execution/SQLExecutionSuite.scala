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

import java.util.Locale
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.parallel.immutable.ParRange
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.classic
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.sql.types._
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.Utils.REDACTION_REPLACEMENT_TEXT

class SQLExecutionSuite extends SparkFunSuite with SQLConfHelper {

  test("concurrent query execution (SPARK-10548)") {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
    val goodSparkContext = new SparkContext(conf)
    try {
      testConcurrentQueryExecution(goodSparkContext)
    } finally {
      goodSparkContext.stop()
    }
  }

  test("concurrent query execution with fork-join pool (SPARK-13747)") {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    import spark.implicits._
    try {
      // Should not throw IllegalArgumentException
      new ParRange(1 to 100).foreach { _ =>
        spark.sparkContext.parallelize(1 to 5).map { i => (i, i) }.toDF("a", "b").count()
      }
    } finally {
      spark.sparkContext.stop()
    }
  }

  /**
   * Trigger SPARK-10548 by mocking a parent and its child thread executing queries concurrently.
   */
  private def testConcurrentQueryExecution(sc: SparkContext): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Initialize local properties. This is necessary for the test to pass.
    sc.getLocalProperties

    // Set up a thread that runs executes a simple SQL query.
    // Before starting the thread, mutate the execution ID in the parent.
    // The child thread should not see the effect of this change.
    var throwable: Option[Throwable] = None
    val child = new Thread {
      override def run(): Unit = {
        try {
          sc.parallelize(1 to 100).map { i => (i, i) }.toDF("a", "b").collect()
        } catch {
          case t: Throwable =>
            throwable = Some(t)
        }

      }
    }
    sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, "anything")
    child.start()
    child.join()

    // The throwable is thrown from the child thread so it doesn't have a helpful stack trace
    throwable.foreach { t =>
      t.setStackTrace(t.getStackTrace ++ Thread.currentThread.getStackTrace)
      throw t
    }
  }


  test("Finding QueryExecution for given executionId") {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._

    var queryExecution: QueryExecution = null

    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        val executionIdStr = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
        if (executionIdStr != null) {
          queryExecution = SQLExecution.getQueryExecution(executionIdStr.toLong)
        }
        SQLExecutionSuite.canProgress = true
      }
    })

    val df = spark.range(1).map { x =>
      while (!SQLExecutionSuite.canProgress) {
        Thread.sleep(1)
      }
      x
    }
    df.collect()

    assert(df.queryExecution === queryExecution)

    spark.stop()
  }

  test("SPARK-32813: Table scan should work in different thread") {
    val executor1 = Executors.newSingleThreadExecutor()
    val executor2 = Executors.newSingleThreadExecutor()
    var session: SparkSession = null
    classic.SparkSession.cleanupAnyExistingSession()

    withTempDir { tempDir =>
      try {
        val tablePath = tempDir.toString + "/table"
        val df = ThreadUtils.awaitResult(Future {
          session = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

          session.createDataFrame(
            session.sparkContext.parallelize(Row(Array(1, 2, 3)) :: Nil),
            StructType(Seq(
              StructField("a", ArrayType(IntegerType, containsNull = false), nullable = false))))
            .write.parquet(tablePath)

          session.read.parquet(tablePath)
        }(ExecutionContext.fromExecutorService(executor1)), 1.minute)

        ThreadUtils.awaitResult(Future {
          assert(df.rdd.collect()(0) === Row(Seq(1, 2, 3)))
        }(ExecutionContext.fromExecutorService(executor2)), 1.minute)
      } finally {
        executor1.shutdown()
        executor2.shutdown()
        session.stop()
      }
    }
  }

  test("Concurrent query execution queryId persists in thread local") {
    SparkSession.getActiveSession.foreach(_.stop())
    val spark = SparkSession.builder().master("local[*]").appName("ignore").getOrCreate()

    try {
      val originalQueryId = spark.sparkContext.getLocalProperty(SQLExecution.QUERY_ID_KEY)
      val parentQE = new QueryExecution(spark.asInstanceOf[classic.SparkSession], OneRowRelation())
      val parentQueryId = parentQE.queryId.toString

      var childQueryId: String = null
      var throwable: Option[Throwable] = None

      val child = new Thread {
        override def run(): Unit = {
          try {
            val childQE =
              new QueryExecution(spark.asInstanceOf[classic.SparkSession], OneRowRelation())
            SQLExecution.withNewExecutionId(childQE) {
              childQueryId = spark.sparkContext.getLocalProperty(SQLExecution.QUERY_ID_KEY)
            }
          } catch {
            case t: Throwable =>
              throwable = Some(t)
          }
        }
      }

      SQLExecution.withNewExecutionId(parentQE) {
        child.start()
        child.join()
      }

      assert(childQueryId != null)
      assert(originalQueryId != childQueryId && parentQueryId != childQueryId)
      // SparkContext originalQueryId should be maintained after child thread
      assert(spark.sparkContext.getLocalProperty(SQLExecution.QUERY_ID_KEY) == originalQueryId)

      throwable.foreach { t =>
        t.setStackTrace(t.getStackTrace ++ Thread.currentThread.getStackTrace)
        throw t
      }
    } finally {
      spark.stop()
    }
  }

  test("QueryId restored for single thread multiple QueryExecutions") {
    // This test simulates a DBSQL query where a single thread creates multiple QueryExecutions
    // queryId should be restored on the thread after all executions complete.
    val spark = SparkSession.builder().master("local[*]").appName("ignore").getOrCreate()

    try {
      val sc = spark.sparkContext
      val originalQueryId = spark.sparkContext.getLocalProperty(SQLExecution.QUERY_ID_KEY)

      def getQueryExecution: QueryExecution =
        new QueryExecution(spark.asInstanceOf[classic.SparkSession], OneRowRelation())

      // analyzeQuery creates df
      val dummyQueryExecution1 = getQueryExecution
      val queryId1 = dummyQueryExecution1.queryId.toString

      Thread.sleep(1) // ensure a time difference in queryId

      // Delta metadata scan triggered in analysis
      val dummyQueryExecution2 = getQueryExecution
      val queryId2 = dummyQueryExecution2.queryId.toString

      // Delta runs collect() on above QE2, triggering SQLExecution.withNewExec
      SQLExecution.withNewExecutionId(dummyQueryExecution2) {
        // Different QueryExecution objects have different queryIds
        assert(dummyQueryExecution2.queryId.toString != queryId1)
        // Same thread, so local queryId should be set to the new id
        assert(sc.getLocalProperty(SQLExecution.QUERY_ID_KEY) == queryId2)
      }

      // After Delta materialize API, should restore to originalQueryId
      assert(sc.getLocalProperty(SQLExecution.QUERY_ID_KEY) == originalQueryId)

      // After analysis, DBSQL executes above analyzed df QE1
      SQLExecution.withNewExecutionId(dummyQueryExecution1) {
        // First execution uses the original queryId
        assert(sc.getLocalProperty(SQLExecution.QUERY_ID_KEY) == queryId1)
      }

      // Restore original null queryId once entire DBSQL query finished
      assert(sc.getLocalProperty(SQLExecution.QUERY_ID_KEY) == null)

    } finally {
      spark.stop()
    }
  }

  test("Multiple execution of same QueryExecution should have unique queryId") {
    val spark = SparkSession.builder().master("local[*]").appName("ignore").getOrCreate()

    try {
      val sc = spark.sparkContext

      def getQueryExecution: QueryExecution =
        new QueryExecution(spark.asInstanceOf[classic.SparkSession], OneRowRelation())

      val dummyQueryExecution = getQueryExecution

      Thread.sleep(1) // ensure a time difference in queryId

      // First execution uses the original queryId from QueryExecution
      val originalQueryId: String = dummyQueryExecution.queryId.toString
      var queryId1: String = null
      SQLExecution.withNewExecutionId(dummyQueryExecution) {
        queryId1 = sc.getLocalProperty(SQLExecution.QUERY_ID_KEY)
        assert(queryId1 == originalQueryId)
      }

      // Second execution generates a new queryId (not stored in QueryExecution)
      var queryId2: String = null
      SQLExecution.withNewExecutionId(dummyQueryExecution) {
        queryId2 = sc.getLocalProperty(SQLExecution.QUERY_ID_KEY)
        assert(queryId2 != originalQueryId)
      }

      assert(queryId1 != queryId2)
    } finally {
      spark.stop()
    }
  }

  test("SPARK-34735: Add modified configs for SQL execution in UI") {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .config("k1", "v1")
      .config(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-Dkey=value")
      .getOrCreate()

    try {
      val index = new AtomicInteger(0)
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
          case start: SparkListenerSQLExecutionStart =>
            if (index.get() == 0 && hasProject(start)) {
              assert(!start.modifiedConfigs.contains("k1"))
              index.incrementAndGet()
            } else if (index.get() == 1 && hasProject(start)) {
              assert(start.modifiedConfigs.contains("k2"))
              assert(start.modifiedConfigs("k2") == "v2")
              assert(start.modifiedConfigs.contains("redaction.password"))
              assert(start.modifiedConfigs("redaction.password") == REDACTION_REPLACEMENT_TEXT)
              assert(!start.modifiedConfigs.contains(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS))
              index.incrementAndGet()
            }
          case _ =>
        }

        private def hasProject(start: SparkListenerSQLExecutionStart): Boolean =
          start.physicalPlanDescription.toLowerCase(Locale.ROOT).contains("project")
      })
      spark.sql("SELECT 1").collect()
      withSQLConf("k2" -> "v2", "redaction.password" -> "123") {
        spark.sql("SELECT 1").collect()
      }
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(index.get() == 2)
    } finally {
      spark.stop()
    }
  }

  test("SPARK-44591: jobTags property") {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    val jobTag = "jobTag"
    try {
      spark.sparkContext.addJobTag(jobTag)

      var jobTags: Option[String] = None
      var sqlJobTags: Set[String] = Set.empty
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          jobTags = Some(jobStart.properties.getProperty(SparkContext.SPARK_JOB_TAGS))
        }
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: SparkListenerSQLExecutionStart =>
              sqlJobTags = e.jobTags
          }
        }
      })

      spark.range(1).collect()

      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(jobTags.get.contains(jobTag))
      assert(sqlJobTags.contains(jobTag))
    } finally {
      spark.sparkContext.removeJobTag(jobTag)
      spark.stop()
    }
  }

  test("jobGroupId property") {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    val JobGroupId = "test-JobGroupId"
    try {
      spark.sparkContext.setJobGroup(JobGroupId, "job Group id")

      var jobGroupIdOpt: Option[String] = None
      var sqlJobGroupIdOpt: Option[String] = None
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          jobGroupIdOpt = Some(jobStart.properties.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
        }

        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: SparkListenerSQLExecutionStart =>
              sqlJobGroupIdOpt = e.jobGroupId
          }
        }
      })

      spark.range(1).collect()

      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(jobGroupIdOpt.contains(JobGroupId))
      assert(sqlJobGroupIdOpt.contains(JobGroupId))
    } finally {
      spark.sparkContext.clearJobGroup()
      spark.stop()
    }
  }
}

object SQLExecutionSuite {
  @volatile var canProgress = false
}
