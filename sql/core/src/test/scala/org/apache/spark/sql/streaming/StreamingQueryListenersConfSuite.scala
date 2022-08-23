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

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.STREAMING_QUERY_LISTENERS
import org.apache.spark.sql.streaming.StreamingQueryListener._


class StreamingQueryListenersConfSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(STREAMING_QUERY_LISTENERS.key,
      Seq(classOf[TestListener].getCanonicalName,
        classOf[TestSQLConfStreamingQueryListener].getCanonicalName).mkString(","))
      .set("spark.aaa", "aaa")
      .set("spark.bbb", "bbb")

  test("test if the configured query listener is loaded") {
    testStream(MemoryStream[Int].toDS)(
      StartStream(),
      StopStream
    )

    spark.sparkContext.listenerBus.waitUntilEmpty()

    assert(TestListener.queryStartedEvent != null)
    assert(TestListener.queryTerminatedEvent != null)
  }

}

object TestListener {
  @volatile var queryStartedEvent: QueryStartedEvent = null
  @volatile var queryTerminatedEvent: QueryTerminatedEvent = null
}

class TestListener(sparkConf: SparkConf) extends StreamingQueryListener {

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    TestListener.queryStartedEvent = event
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {}

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    TestListener.queryTerminatedEvent = event
  }
}


class TestSQLConfStreamingQueryListener extends StreamingQueryListener {

  val sqlConf = SQLConf.get
  assert(sqlConf.getConfString("spark.aaa") == "aaa")
  assert(sqlConf.getConfString("spark.bbb") == "bbb")

  override def onQueryStarted(event: QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: QueryProgressEvent): Unit = {}

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
}
