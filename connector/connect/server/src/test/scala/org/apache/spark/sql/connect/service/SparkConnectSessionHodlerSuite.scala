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

import scala.collection.JavaConverters._

import com.google.common.collect.{Lists, Maps}
import org.scalatest.time.SpanSugar._

import org.apache.spark.api.python.SimplePythonFunction
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.planner.StreamingForeachBatchHelper
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

  // create python process and run cloudpickle,
  // also possible to pass the temp file to the python process

//  private def pythonProcessTestFcn(pidPath: Path, functionPath: Path): Unit = {
//    val pb = new ProcessBuilder()
//    val pythonScript =
//      s"""
//         |import os
//         |from pyspark.serializers import CloudPickleSerializer
//         |
//         |def func():
//         |    with open("${pidPath.toString}"), 'w') as f:
//         |        f.write(str(os.getpid()))
//         |
//         |bytes = CloudPickleSerializer().dumps(func)
//         |with open("${functionPath.toString}"), 'w') as f:
//         |    f.write(bytes)
//         |
//         |exit()
//         |""".stripMargin
//    pb.command(pythonExec, "-c", pythonScript)
//    pb.environment().put(
//      "PYTHONPATH",
//      PythonUtils.sparkPythonPath
//    )
//    val process = pb.start()
//    process.waitFor()
//  }

  private def dummyPythonFunction(sessionHolder: SessionHolder): SimplePythonFunction = {
    SimplePythonFunction(
      command = Array.emptyByteArray,
      envVars = Maps.newHashMap(),
      pythonIncludes = sessionHolder.artifactManager.getSparkConnectPythonIncludes.asJava,
      pythonExec = sys.env.getOrElse("PYSPARK_PYTHON",
        sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", "python3")),
      pythonVer = "3.9",
      broadcastVars = Lists.newArrayList(),
      accumulator = null)
  }

  test("foreachBatch process: process terminates after query is stopped") {
    val sessionHolder = SessionHolder.forTesting(spark)

//
//    val query1 = sessionHolder.session
//      .readStream
//      .format("rate")
//      .load()
//      .writeStream
//      .format("memory")
//      .queryName("foreachBatchterminationtest1")
//      .start()
//
//    eventually(timeout(30.seconds)) {
//      assert(query1.status.isDataAvailable)
//      assert(query1.recentProgress.nonEmpty) // Query made progress.
//    }
//
//    println("====wei query1 made progress")

    SparkConnectService.start(spark.sparkContext)
    val pythonFn = dummyPythonFunction(sessionHolder)
    val (fn, cleaner) =
      StreamingForeachBatchHelper.testPythonForeachBatchWrapper(pythonFn, sessionHolder)

    println("====wei before query creation====")

    val query = sessionHolder.session
      .readStream
      .format("rate")
      .load()
      .writeStream
      .format("memory")
      .queryName("foreachBatch_termination_test")
      .foreachBatch(fn)
      .start()

    println("====wei query created====")

    sessionHolder.streamingForeachBatchRunnerCleanerCache.registerCleanerForQuery(
      query,
      cleaner
    )

    println("====wei query cleaner registered====")

    // assert one microbatch is ran
    eventually(timeout(30.seconds)) {
      assert(query.status.isDataAvailable)
//      assert(query.recentProgress.nonEmpty) // Query made progress.
    }
    println("====wei query made progress====")
    // assert python process is running
    assert(StreamingForeachBatchHelper.pythonRunner.isDefined)
    assert(StreamingForeachBatchHelper.pythonRunner.get.pythonWorker.isDefined)
    val worker = StreamingForeachBatchHelper.pythonRunner.get.pythonWorker.get
    assert(!worker.isStopped())
    // stop query
    query.stop()
    // assert python process is not running
    assert(worker.isStopped())
  }

  test("listener process: process terminates after listener is dropped") {
    val sessionHolder = SessionHolder.forTesting(spark)
    SparkConnectService.start(spark.sparkContext)

    val pythonFn = dummyPythonFunction(sessionHolder)

    val query = sessionHolder.session
      .readStream
      .format("rate")
      .load()
      .writeStream
      .format("memory")
      .queryName("listener_removeListener_test")
      .start()

    // assert one microbatch is ran
    eventually(timeout(30.seconds)) {
      assert(query.status.isDataAvailable)
      assert(query.recentProgress.nonEmpty) // Query made progress.
    }
    println("====wei query made progress====")
    // assert python process is running
    assert(StreamingForeachBatchHelper.pythonRunner.isDefined)
    assert(StreamingForeachBatchHelper.pythonRunner.get.pythonWorker.isDefined)
    val worker = StreamingForeachBatchHelper.pythonRunner.get.pythonWorker.get
    assert(!worker.isStopped())
    // stop query
    query.stop()
    // assert python process is not running
    assert(worker.isStopped())
  }
}
