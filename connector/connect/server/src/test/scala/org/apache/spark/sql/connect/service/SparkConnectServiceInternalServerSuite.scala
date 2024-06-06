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

import java.net.ServerSocket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Semaphore

import scala.collection.mutable

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.connect.SparkConnectPlugin
import org.apache.spark.sql.connect.config.Connect.{CONNECT_GRPC_BINDING_PORT, CONNECT_GRPC_PORT_MAX_RETRIES}
import org.apache.spark.util.Utils

class SparkConnectServiceInternalServerSuite extends SparkFunSuite with LocalSparkContext {

  override def afterEach(): Unit = {
    super.afterEach()
    SparkConnectServiceLifeCycleListener.reset()
  }

  test("The SparkConnectService will retry using different ports in case of conflicts") {
    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
    sc = new SparkContext(conf)

    // 1. By default there is no retry, the SparkConnectService will fail to start
    //    if the port is already in use.
    val startPort = 15002
    withSparkEnvConfs((CONNECT_GRPC_BINDING_PORT.key, startPort.toString)) {
      withPortOccupied(startPort, startPort) {
        val portConflicts = intercept[Throwable] {
          SparkConnectService.start(sc)
        }
        portConflicts.printStackTrace()
        assert(Utils.isBindCollision(portConflicts))
      }
    }

    // 2. Enable the port retry, the SparkConnectService will retry using different ports
    //    until it finds an available port before reaching the maximum number of retries.
    withSparkEnvConfs(
      (CONNECT_GRPC_BINDING_PORT.key, startPort.toString),
      (CONNECT_GRPC_PORT_MAX_RETRIES.key, "3")) {
      // 15002, 15003, 15004 occupied
      withPortOccupied(startPort, startPort + 2) {
        SparkConnectService.start(sc)
        assert(SparkConnectService.started)
        assert(SparkConnectService.server.getPort == startPort + 3) // 15005 available
        SparkConnectService.stop()
      }
    }

    // 3. It will fail if not able to find an available port
    //    before reaching the maximum number of retries.
    withSparkEnvConfs(
      (CONNECT_GRPC_BINDING_PORT.key, startPort.toString),
      (CONNECT_GRPC_PORT_MAX_RETRIES.key, "1")) {
      // 15002, 15003 occupied but only retried on 15003 and reach the maximum number of retries
      withPortOccupied(startPort, startPort + 1) {
        val portConflicts = intercept[Throwable] {
          SparkConnectService.start(sc)
        }
        portConflicts.printStackTrace()
        assert(Utils.isBindCollision(portConflicts))
      }
    }

    // 4. The value of port will be validated before the service starts
    Seq(
      (CONNECT_GRPC_BINDING_PORT.key, (1024 - 1).toString),
      (CONNECT_GRPC_BINDING_PORT.key, (65535 + 1).toString)).foreach(conf => {
      withSparkEnvConfs(conf) {
        val invalidPort = intercept[IllegalArgumentException] {
          SparkConnectService.start(sc)
        }
        assert(
          invalidPort.getMessage.contains(
            "requirement failed: startPort should be between 1024 and 65535 (inclusive)," +
              " or 0 for a random free port."))
      }
    })
  }

  test("The SparkConnectService will post events for each pair of start and stop") {
    // Future validations when listener receive the `SparkListenerConnectServiceStarted` event
    val startedEventValidations: CopyOnWriteArrayList[(String, Boolean)] =
      new CopyOnWriteArrayList[(String, Boolean)]()
    val startedEventSignal = new Semaphore(0)
    SparkConnectServiceLifeCycleListener.checksOnServiceStartedEvent = Some(
      Seq(
        _ => {
          startedEventSignal.release()
          startedEventValidations.add(
            ("The listener should receive the `SparkListenerConnectServiceStarted` event.", true))
        },
        _ => {
          startedEventValidations.add(
            (
              "The server should has already been started" +
                " if the listener receives the `SparkListenerConnectServiceStarted` event.",
              SparkConnectService.started &&
                !SparkConnectService.stopped &&
                SparkConnectService.server != null))
        },
        serviceStarted => {
          startedEventValidations.add(
            (
              "The SparkConnectService should post it's address " +
                "by the `SparkListenerConnectServiceStarted` event",
              serviceStarted.bindingPort == SparkConnectService.server.getPort &&
                serviceStarted.hostAddress == SparkConnectService.hostAddress))
        }))

    // Future validations when listener receive the `SparkListenerConnectServiceEnd` event
    val endEventValidations: CopyOnWriteArrayList[(String, Boolean)] =
      new CopyOnWriteArrayList[(String, Boolean)]()
    val endEventSignal = new Semaphore(0)
    SparkConnectServiceLifeCycleListener.checksOnServiceEndEvent = Some(
      Seq(
        _ => {
          endEventSignal.release()
          startedEventValidations.add(
            ("The listener should receive the `SparkListenerConnectServiceEnd` event.", true))
        },
        _ => {
          endEventValidations.add(
            (
              "The server has already been stopped" +
                " if the listener receives the `SparkListenerConnectServiceEnd` event.",
              SparkConnectService.stopped &&
                !SparkConnectService.started &&
                SparkConnectService.server.isShutdown))
        },
        serviceEnd => {
          endEventValidations.add(
            (
              "The SparkConnectService should post it's address " +
                "by the `SparkListenerConnectServiceEnd` event",
              serviceEnd.bindingPort == SparkConnectService.server.getPort &&
                serviceEnd.hostAddress == SparkConnectService.hostAddress))
        }))

    val conf = new SparkConf()
      .setAppName(getClass().getName())
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
    sc = new SparkContext(conf)

    val listenerInstance = new SparkConnectServiceLifeCycleListener()
    sc.addSparkListener(listenerInstance)

    // Start the SparkConnectService and wait for the listener
    // to receive the `SparkListenerConnectServiceStarted` event.
    SparkConnectService.start(sc)
    startedEventSignal.acquire()
    // Now the listener should have already received the `SparkListenerConnectServiceStarted` event.

    // The internal server of SparkConnectService should has
    // already been created and started in this time.
    assert(SparkConnectService.started && SparkConnectService.server != null)

    // The event `SparkListenerConnectServiceStarted` should be posted
    // during the startup of the SparkConnectService.
    assert(listenerInstance.serviceStartedEvents.size() == 1)
    // The server should already been started when the listener receive the event
    // and the server address should be the same as the address of service
    startedEventValidations.forEach { case (msg, validated) =>
      assert(validated, msg)
    }
    // In the meanwhile, no any end event should be posted
    assert(listenerInstance.serviceEndEvents.size() == 0)

    // The listener is able to get the SparkConf from the event
    val event = listenerInstance.serviceStartedEvents.get(0)
    assert(event.sparkConf != null)
    val sparkConf = event.sparkConf
    assert(sparkConf.contains("spark.driver.host"))
    assert(sparkConf.contains("spark.app.id"))

    // Try to start an already started SparkConnectService
    SparkConnectService.start(sc)
    // The listener should still receive only one started event
    // because the server has not been stopped yet, and there won't be duplicated service start
    assert(listenerInstance.serviceStartedEvents.size() == 1)

    // Stop the SparkConnectService
    SparkConnectService.stop()
    assert(SparkConnectService.stopped)
    // The listener should receive the `SparkListenerConnectServiceEnd` event
    endEventSignal.acquire()

    // The event `SparkListenerConnectServiceEnd` should be posted and received by the listener
    assert(listenerInstance.serviceEndEvents.size() == 1)
    // The server should already been stopped when the listener receive the event
    // and the server address should be the same as the address of service
    endEventValidations.forEach { case (msg, validated) =>
      assert(validated, msg)
    }

    // Try to stop an already stopped SparkConnectService
    SparkConnectService.stop()
    // The listener should still receive only one end event,
    // no duplicated `SparkListenerConnectServiceEnd` event posted
    assert(listenerInstance.serviceEndEvents.size() == 1)
  }

  test("SparkConnectPlugin will post started and end events that can be received by listeners") {
    // Future validations when listener receive the `SparkListenerConnectServiceStarted` event
    val startedEventSignal = new Semaphore(0)
    SparkConnectServiceLifeCycleListener.checksOnServiceStartedEvent = Some(Seq(_ => {
      startedEventSignal.release()
    }))

    // Future validations when listener receive the `SparkListenerConnectServiceEnd` event
    val endEventSignal = new Semaphore(0)
    SparkConnectServiceLifeCycleListener.checksOnServiceEndEvent = Some(Seq(_ => {
      endEventSignal.release()
    }))

    val conf = new SparkConf()
      .setAppName(getClass().getName())
      // Start the SparkConnectService from SparkConnectPlugin
      .set(PLUGINS, Seq(classOf[SparkConnectPlugin].getName()))
      // In this case, the listener need to be registered via the configuration
      // otherwise the listener will not be able to receive the events that post during
      // the initialization of the SparkConnectPlugin
      .set(EXTRA_LISTENERS, Seq(classOf[SparkConnectServiceLifeCycleListener].getName()))
      .set(SparkLauncher.SPARK_MASTER, "local[1]")

    // Create the SparkContext, initialize the SparkConnectPlugin and start the SparkConnectService
    sc = new SparkContext(conf)

    val listenerInstance = SparkConnectServiceLifeCycleListener.currentInstance
    assert(listenerInstance != null)
    // The internal server of SparkConnectService should has
    // already been created and started during the initializing of the SparkConnectPlugin.
    assert(SparkConnectService.started && SparkConnectService.server != null)
    // The event `SparkListenerConnectServiceStarted` should be posted and received by the listener
    startedEventSignal.acquire()
    // Only one `SparkListenerConnectServiceStarted` event should be received by the listener
    assert(listenerInstance.serviceStartedEvents.size() == 1)

    // Stop the SparkContext, the SparkConnectService will be stopped during the shutdown of
    // SparkConnectPlugin and the message will be posted to the listener via active ListenerBus.
    // This requires the ListenerBus can accept events if the SparkPlugins has not been shutdown.
    sc.stop()
    assert(SparkConnectService.stopped)
    // The listener should receive the `SparkListenerConnectServiceEnd` event
    endEventSignal.acquire()
  }

  def withPortOccupied(startPort: Int, endPort: Int)(f: => Unit): Unit = {
    val startedServers = new mutable.ArrayBuffer[ServerSocket]()
    try {
      for (toBeOccupiedPort <- startPort to endPort) {
        val server = new ServerSocket(toBeOccupiedPort)
        startedServers += server
      }
      f
    } finally {
      startedServers.foreach(server => {
        try {
          server.close()
        } catch {
          case _: Throwable =>
        }
      })
    }
  }
}

private class SparkConnectServiceLifeCycleListener extends SparkListener {

  SparkConnectServiceLifeCycleListener.currentInstance = this

  val serviceStartedEvents: CopyOnWriteArrayList[SparkListenerConnectServiceStarted] =
    new CopyOnWriteArrayList[SparkListenerConnectServiceStarted]()
  val serviceEndEvents: CopyOnWriteArrayList[SparkListenerConnectServiceEnd] =
    new CopyOnWriteArrayList[SparkListenerConnectServiceEnd]()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case serviceStarted: SparkListenerConnectServiceStarted =>
        serviceStartedEvents.add(serviceStarted)
        SparkConnectServiceLifeCycleListener.checksOnServiceStartedEvent.foreach { checks =>
          checks.foreach(_(serviceStarted))
        }
      case serviceEnd: SparkListenerConnectServiceEnd =>
        serviceEndEvents.add(serviceEnd)
        SparkConnectServiceLifeCycleListener.checksOnServiceEndEvent.foreach { checks =>
          checks.foreach(_(serviceEnd))
        }
    }
  }
}

private object SparkConnectServiceLifeCycleListener {

  var currentInstance: SparkConnectServiceLifeCycleListener = _
  var checksOnServiceStartedEvent: Option[Seq[(SparkListenerConnectServiceStarted) => Unit]] =
    None
  var checksOnServiceEndEvent: Option[Seq[(SparkListenerConnectServiceEnd) => Unit]] = None

  def reset(): Unit = {
    currentInstance = null
    checksOnServiceStartedEvent = None
    checksOnServiceEndEvent = None
  }
}
