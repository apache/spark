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
package org.apache.spark.ml.recommendation.logistic.local

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

/**
 * @author ezamyatin
 * */
private[ml] object ParItr {
  def foreach[A](iterator: Iterator[A], cpus: Int, fn: A => Unit): Unit = {
    val inQueue = new LinkedBlockingQueue[A](cpus * 5)
    val totalCounter = new AtomicLong(0)
    val error = new AtomicReference[Exception](null)
    val end = new AtomicBoolean(false)
    val mainThread = Thread.currentThread()

    val threads = Array.fill(cpus) {
      new Thread(() => {
        try {
          while (!end.get() || inQueue.size() > 0) {
            fn(inQueue.take)
            totalCounter.decrementAndGet
          }
          mainThread.interrupt()
        } catch {
          case _: InterruptedException =>
          case e: Exception => error.set(e)
        }
      })
    }

    try {
      threads.foreach(_.start())
      iterator.foreach(e => {
        var ok = false
        while (!ok && error.get() == null) {
          ok = inQueue.offer(e, 10, TimeUnit.SECONDS)
        }
        totalCounter.incrementAndGet
      })

      end.set(true)
      while (totalCounter.get > 0 && error.get() == null) {
        try {
          Thread.sleep(1000)
        } catch {
          case _: InterruptedException =>
        }
      }
    } finally {
      threads.foreach(_.interrupt())
      threads.foreach{t =>
        try {
          t.join()
        } catch {
          case _: InterruptedException => t.join()
        }
      }
    }

    if (error.get() != null) {
      throw error.get()
    }

  }
}