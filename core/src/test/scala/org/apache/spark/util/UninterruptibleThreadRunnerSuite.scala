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

import org.apache.spark.SparkFunSuite

class UninterruptibleThreadRunnerSuite extends SparkFunSuite {
  private var runner: UninterruptibleThreadRunner = null

  override def beforeEach(): Unit = {
    runner = new UninterruptibleThreadRunner("ThreadName")
  }

  override def afterEach(): Unit = {
    runner.shutdown()
  }

  test("runUninterruptibly should switch to UninterruptibleThread") {
    assert(!Thread.currentThread().isInstanceOf[UninterruptibleThread])
    var isUninterruptibleThread = false
    runner.runUninterruptibly {
      isUninterruptibleThread = Thread.currentThread().isInstanceOf[UninterruptibleThread]
    }
    assert(isUninterruptibleThread, "The runner task must run in UninterruptibleThread")
  }

  test("runUninterruptibly should not add new UninterruptibleThread") {
    var isInitialUninterruptibleThread = false
    var isRunnerUninterruptibleThread = false
    val t = new UninterruptibleThread("test") {
      override def run(): Unit = {
        runUninterruptibly {
          val initialThread = Thread.currentThread()
          isInitialUninterruptibleThread = initialThread.isInstanceOf[UninterruptibleThread]
          runner.runUninterruptibly {
            val runnerThread = Thread.currentThread()
            isRunnerUninterruptibleThread = runnerThread.isInstanceOf[UninterruptibleThread]
            assert(runnerThread.eq(initialThread))
          }
        }
      }
    }
    t.start()
    t.join()
    assert(isInitialUninterruptibleThread,
      "The initiator must already run in UninterruptibleThread")
    assert(isRunnerUninterruptibleThread, "The runner task must run in UninterruptibleThread")
  }
}
