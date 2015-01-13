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

package org.apache.spark.util

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.concurrent.Eventually._
import org.scalatest.FunSuite

class EventLoopSuite extends FunSuite {

  test("EventLoop") {
    val buffer = new mutable.ArrayBuffer[Int] with mutable.SynchronizedBuffer[Int]
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        buffer += event
      }

      override def onError(e: Throwable): Unit = {}
    }
    eventLoop.start()
    (1 to 100).foreach(eventLoop.post)
    eventually(timeout(5 seconds), interval(200 millis)) {
      assert((1 to 100) === buffer.toSeq)
    }
    eventLoop.stop()
  }

  test("EventLoop: start and stop") {
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {}

      override def onError(e: Throwable): Unit = {}
    }
    assert(false === eventLoop.isActive)
    eventLoop.start()
    assert(true === eventLoop.isActive)
    eventLoop.stop()
    assert(false === eventLoop.isActive)
  }

  test("EventLoop: onError") {
    val e = new RuntimeException("Oops")
    val receivedError = new AtomicReference[Throwable]()
    val eventLoop = new EventLoop[Int]("test") {

      override def onReceive(event: Int): Unit = {
        throw e
      }

      override def onError(e: Throwable): Unit = {
        receivedError.set(e)
      }
    }
    eventLoop.start()
    eventLoop.post(1)
    eventually(timeout(5 seconds), interval(200 millis)) {
      assert(e === receivedError.get)
    }
    eventLoop.stop()
  }
}
