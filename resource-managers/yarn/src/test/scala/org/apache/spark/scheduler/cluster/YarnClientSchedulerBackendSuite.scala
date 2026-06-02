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
package org.apache.spark.scheduler.cluster

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.mockito.ArgumentMatchers.{anyBoolean, anyLong}
import org.mockito.Mockito.{mock, when}

import org.apache.spark._
import org.apache.spark.deploy.yarn.Client
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, TaskSchedulerImpl}

class YarnClientSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext {

  test("SPARK-57191: MonitorThread calls sc.stop() on unexpected exception") {
    val stopCalled = new CountDownLatch(1)
    sc = new SparkContext("local", "test", new SparkConf().set("spark.testing", "true"))
    sc.addSparkListener(new SparkListener {
      override def onApplicationEnd(e: SparkListenerApplicationEnd): Unit =
        stopCalled.countDown()
    })

    val backend = new YarnClientSchedulerBackend(
      sc.taskScheduler.asInstanceOf[TaskSchedulerImpl], sc)

    // Simulate MonitorThread hitting a fatal error (e.g., credential expiry, network failure)
    val mockClient = mock(classOf[Client])
    when(mockClient.monitorApplication(anyBoolean(), anyBoolean(), anyLong()))
      .thenThrow(new RuntimeException("Simulated failure"))

    // Use reflection since client/appId are private and MonitorThread is an inner class
    val clientField = backend.getClass.getDeclaredFields.find(_.getName.endsWith("client")).get
    clientField.setAccessible(true)
    clientField.set(backend, mockClient)

    val appIdField = classOf[YarnSchedulerBackend].getDeclaredField("appId")
    appIdField.setAccessible(true)
    appIdField.set(backend,
      Some(org.apache.hadoop.yarn.api.records.ApplicationId.newInstance(0L, 1)))

    val monitorMethod = backend.getClass.getDeclaredMethod("asyncMonitorApplication")
    monitorMethod.setAccessible(true)
    monitorMethod.invoke(backend).asInstanceOf[Thread].start()

    assert(stopCalled.await(10, TimeUnit.SECONDS),
      "sc.stop() was not called after MonitorThread hit an unexpected exception")
  }
}
