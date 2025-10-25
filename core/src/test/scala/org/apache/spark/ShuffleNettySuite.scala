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

package org.apache.spark

import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfterAll, Tag}

import org.apache.spark.network.util.IOMode
import org.apache.spark.util.Utils

abstract class ShuffleNettySuite extends ShuffleSuite with BeforeAndAfterAll {

  // This test suite should run all tests in ShuffleSuite with Netty shuffle mode.

  def ioMode: IOMode = IOMode.NIO
  def shouldRunTests: Boolean = true
  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set("spark.shuffle.io.mode", ioMode.toString)
  }

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)(
    implicit pos: Position): Unit = {
    if (!shouldRunTests) {
      ignore(s"$testName [disabled on ${Utils.osName} with $ioMode]")(testBody)
    } else {
      super.test(testName, testTags: _*) {testBody}
    }
  }
}

class ShuffleNettyNioSuite extends ShuffleNettySuite

class ShuffleNettyEpollSuite extends ShuffleNettySuite {
  override def shouldRunTests: Boolean = Utils.isLinux
  override def ioMode: IOMode = IOMode.EPOLL
}

class ShuffleNettyKQueueSuite extends ShuffleNettySuite {
  override def shouldRunTests: Boolean = Utils.isMac
  override def ioMode: IOMode = IOMode.KQUEUE
}

class ShuffleNettyAutoSuite extends ShuffleNettySuite {
  override def ioMode: IOMode = IOMode.AUTO
}
