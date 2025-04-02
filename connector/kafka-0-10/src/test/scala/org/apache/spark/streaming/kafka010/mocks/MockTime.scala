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

package org.apache.spark.streaming.kafka010.mocks

import java.lang
import java.util.concurrent._
import java.util.function.Supplier

import org.apache.kafka.common.utils.Time

/**
 * A class used for unit testing things which depend on the Time interface.
 *
 * This class never manually advances the clock, it only does so when you call
 *   sleep(ms)
 *
 * It also comes with an associated scheduler instance for managing background tasks in
 * a deterministic way.
 */
private[kafka010] class MockTime(@volatile private var currentMs: Long) extends Time {

  val scheduler = new MockScheduler(this)

  def this() = this(System.currentTimeMillis)

  override def milliseconds: Long = currentMs

  override def hiResClockMs(): Long = milliseconds

  override def nanoseconds: Long =
    TimeUnit.NANOSECONDS.convert(currentMs, TimeUnit.MILLISECONDS)

  override def sleep(ms: Long): Unit = {
    this.currentMs += ms
    scheduler.tick(ms, TimeUnit.MILLISECONDS)
  }

  override def waitObject(obj: Any, condition: Supplier[lang.Boolean], timeoutMs: Long): Unit =
    throw new UnsupportedOperationException

  override def toString(): String = s"MockTime($milliseconds)"

}
