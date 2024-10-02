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

import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

/**
 * @author ezamyatin
 * */
private[ml] object ParItr {
  def foreach[A](iterator: Iterator[A], cpus: Int, fn: A => Unit): Unit = {
    val inQueue = new LinkedBlockingQueue[A](cpus * 5)
    val totalCounter = new AtomicLong(0)
    val threads = Array.fill(cpus) {
      new Thread(() => {
        try {
          while (true) {
            fn(inQueue.take)
            totalCounter.decrementAndGet
          }
        } catch {
          case _: InterruptedException => _
        }
      }.asInstanceOf[Runnable])
    }

    threads.foreach(_.start())

    iterator.foreach(e => {
      try {
        inQueue.put(e)
      } catch {
        case ex: InterruptedException => throw new RuntimeException(ex)
      }
      totalCounter.incrementAndGet
    })

    while (totalCounter.get > 0) {
      try {
        Thread.sleep(1000)
      } catch {
        case ex: InterruptedException => throw new RuntimeException(ex)
      }
    }

    try {
      threads.foreach(_.interrupt())
      threads.foreach(_.join())
    } catch {
      case e: InterruptedException => throw new RuntimeException(e)
    }

    assert(totalCounter.get == 0)
    assert(!iterator.hasNext)
  }
}