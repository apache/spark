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

package org.apache.spark.deploy

import scala.collection.mutable

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkListenerExecutorAdded, SparkListener}
import org.apache.spark.{SparkContext, LocalSparkContext}

class LogUrlsStandaloneSuite extends FunSuite with LocalSparkContext with BeforeAndAfter {

  /** Length of time to wait while draining listener events. */
  val WAIT_TIMEOUT_MILLIS = 10000

  before {
    sc = new SparkContext("local-cluster[2,1,512]", "test")
  }

  test("verify log urls get propagated from workers") {
    val listener = new SaveExecutorInfo
    sc.addSparkListener(listener)

    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.map(_.toString)
    rdd2.setName("Target RDD")
    rdd2.count()

    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    listener.addedExecutorInfos.values.foreach { info =>
      assert(info.logUrlMap.nonEmpty)
    }
  }

  private class SaveExecutorInfo extends SparkListener {
    val addedExecutorInfos = mutable.Map[String, ExecutorInfo]()

    override def onExecutorAdded(executor: SparkListenerExecutorAdded) {
      addedExecutorInfos(executor.executorId) = executor.executorInfo
    }
  }
}
