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
package org.apache.spark.sql.streaming

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.test.{SQLTestUtilsBase, TestSparkSession}

class StreamingQueryStopSuite extends SparkFunSuite with SQLTestUtilsBase with BeforeAndAfterEach {

  import testImplicits._

  private var session: SparkSession = _

  override protected def spark: SparkSession = session

  test("Stopping spark session should stop underlying query") {
    val input = MemoryStream[Int](implicitly[Encoder[Int]], spark.sqlContext)
    val q = startQuery(input.toDS(), "q1")
    assert(q.isActive)
    assert(q.exception.isEmpty)
    input.addData(1, 2, 3)
    q.processAllAvailable()
    spark.stop()
    assert(!q.isActive)
    assert(q.exception.isEmpty)
  }

  test("Stopping spark session should stop all the streaming queries") {
    val input1 = MemoryStream[Int](implicitly[Encoder[Int]], spark.sqlContext)
    val input2 = MemoryStream[Int](implicitly[Encoder[Int]], spark.sqlContext)
    val q1 = startQuery(input1.toDS(), "q1")
    val q2 = startQuery(input2.toDS(), "q2")
    assert(q1.isActive)
    assert(q1.exception.isEmpty)
    assert(q2.isActive)
    assert(q2.exception.isEmpty)
    input1.addData(1, 2, 3)
    input2.addData(1, 3, 5)
    q1.processAllAvailable()
    q2.processAllAvailable()
    spark.stop()
    assert(!q1.isActive)
    assert(!q2.isActive)
    assert(q1.exception.isEmpty)
    assert(q2.exception.isEmpty)
  }

  test("Stopping spark context should stop all the streaming queries") {
    val input1 = MemoryStream[Int](implicitly[Encoder[Int]], spark.sqlContext)
    val input2 = MemoryStream[Int](implicitly[Encoder[Int]], spark.sqlContext)
    val q1 = startQuery(input1.toDS(), "q1")
    val q2 = startQuery(input2.toDS(), "q2")
    input1.addData(1, 2, 3)
    input2.addData(1, 3, 5)
    q1.processAllAvailable()
    q2.processAllAvailable()
    sparkContext.stop()
    assert(!q1.isActive)
    assert(!q2.isActive)
    assert(q1.exception.isEmpty)
    assert(q2.exception.isEmpty)
  }


  override protected def beforeEach(): Unit = {
    super.beforeEach()
    session = new TestSparkSession(new SparkConf())
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    session.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def startQuery(ds: Dataset[Int], name: String): StreamingQuery = {
    ds.writeStream
      .queryName(name)
      .format("memory")
      .start()
  }
}
