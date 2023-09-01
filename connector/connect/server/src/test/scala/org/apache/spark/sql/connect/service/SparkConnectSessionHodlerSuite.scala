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

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.collect.Lists
import org.scalatest.time.SpanSugar._

import org.apache.spark.api.python.{PythonUtils, SimplePythonFunction}
import org.apache.spark.sql.IntegratedUDFTestUtils
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.planner.{PythonStreamingQueryListener, StreamingForeachBatchHelper}
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectSessionHolderSuite extends SharedSparkSession {

  test("DataFrame cache: Successful put and get") {
    val sessionHolder = SessionHolder.forTesting(spark)
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
    val sessionHolder = SessionHolder.forTesting(spark)
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
    val sessionHolder = SessionHolder.forTesting(spark)
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

  private def dummyPythonFunction(sessionHolder: SessionHolder): SimplePythonFunction = {
    // Needed because pythonPath in PythonWorkerFactory doesn't include test spark home
    val sparkPythonPath = PythonUtils.mergePythonPaths(
      Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator),
      Seq(sparkHome, "python", "lib", PythonUtils.PY4J_ZIP_NAME).mkString(File.separator))

    SimplePythonFunction(
      command = Array.emptyByteArray,
      envVars = mutable.Map("PYTHONPATH" -> sparkPythonPath).asJava,
      pythonIncludes = sessionHolder.artifactManager.getSparkConnectPythonIncludes.asJava,
      pythonExec = IntegratedUDFTestUtils.pythonExec,
      pythonVer = IntegratedUDFTestUtils.pythonVer,
      broadcastVars = Lists.newArrayList(),
      accumulator = null)
  }

  test("python foreachBatch process: process terminates after query is stopped") {
    val sessionHolder = SessionHolder.forTesting(spark)
    SparkConnectService.start(spark.sparkContext)

    val pythonFn = dummyPythonFunction(sessionHolder)
    val (fn1, cleaner1) =
      StreamingForeachBatchHelper.testPythonForeachBatchWrapper(pythonFn, sessionHolder)
    val (fn2, cleaner2) =
      StreamingForeachBatchHelper.testPythonForeachBatchWrapper(pythonFn, sessionHolder)

    val query1 = sessionHolder.session.readStream
      .format("rate")
      .load()
      .writeStream
      .format("memory")
      .queryName("foreachBatch_termination_test_q1")
      .foreachBatch(fn1)
      .start()

    val query2 = sessionHolder.session.readStream
      .format("rate")
      .load()
      .writeStream
      .format("memory")
      .queryName("foreachBatch_termination_test_q2")
      .foreachBatch(fn2)
      .start()

    sessionHolder.streamingForeachBatchRunnerCleanerCache.registerCleanerForQuery(
      query1,
      cleaner1)
    sessionHolder.streamingForeachBatchRunnerCleanerCache.registerCleanerForQuery(
      query2,
      cleaner2)

    assert(cleaner1.runner.pythonWorker.isDefined)
    val worker1 = cleaner1.runner.pythonWorker.get
    assert(cleaner2.runner.pythonWorker.isDefined)
    val worker2 = cleaner2.runner.pythonWorker.get

    // assert both python processes are running
    assert(!worker1.isStopped())
    assert(!worker2.isStopped())
    // stop query1
    query1.stop()
    // assert query1's python process is not running
    eventually(timeout(30.seconds)) {
      assert(worker1.isStopped())
      assert(!worker2.isStopped())
    }

    // stop query2
    query2.stop()
    // assert query2's python process is not running
    eventually(timeout(30.seconds)) {
      assert(worker2.isStopped())
    }
  }

  test("python listener process: process terminates after listener is removed") {
    val sessionHolder = SessionHolder.forTesting(spark)
    SparkConnectService.start(spark.sparkContext)

    val pythonFn = dummyPythonFunction(sessionHolder)

    val id1 = "listener_removeListener_test_1"
    val id2 = "listener_removeListener_test_2"
    val listener1 = PythonStreamingQueryListener.forTesting(pythonFn, sessionHolder)
    val listener2 = PythonStreamingQueryListener.forTesting(pythonFn, sessionHolder)

    sessionHolder.cacheListenerById(id1, listener1)
    sessionHolder.session.streams.addListener(listener1)
    sessionHolder.cacheListenerById(id2, listener2)
    sessionHolder.session.streams.addListener(listener2)

    assert(listener1.runner.pythonWorker.isDefined)
    val worker1 = listener1.runner.pythonWorker.get
    assert(listener2.runner.pythonWorker.isDefined)
    val worker2 = listener2.runner.pythonWorker.get

    // assert both python processes are running
    assert(!worker1.isStopped())
    assert(!worker2.isStopped())

    // remove listener1
    sessionHolder.session.streams.removeListener(listener1)
    sessionHolder.removeCachedListener(id1)
    // assert listener1's python process is not running
    eventually(timeout(30.seconds)) {
      assert(worker1.isStopped())
      assert(!worker2.isStopped())
    }

    // remove listener2
    sessionHolder.session.streams.removeListener(listener2)
    sessionHolder.removeCachedListener(id2)
    // assert listener2's python process is not running
    eventually(timeout(30.seconds)) {
      assert(worker2.isStopped())
      assert(sessionHolder.session.streams.listListeners().isEmpty)
    }
  }
}
