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

import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
 * [[UninterruptibleThreadRunner]] ensures that all tasks are running in an
 * [[UninterruptibleThread]]. A good example is Kafka consumer usage.
 */
private[spark] class UninterruptibleThreadRunner(threadName: String) {
  private val thread = Executors.newSingleThreadExecutor((r: Runnable) => {
    val t = new UninterruptibleThread(threadName) {
      override def run(): Unit = {
        r.run()
      }
    }
    t.setDaemon(true)
    t
  })
  private val execContext = ExecutionContext.fromExecutorService(thread)

  def runUninterruptibly[T](body: => T): T = {
    if (!Thread.currentThread.isInstanceOf[UninterruptibleThread]) {
      val future = Future {
        body
      }(execContext)
      ThreadUtils.awaitResult(future, Duration.Inf)
    } else {
      body
    }
  }

  def shutdown(): Unit = {
    thread.shutdown()
  }
}
