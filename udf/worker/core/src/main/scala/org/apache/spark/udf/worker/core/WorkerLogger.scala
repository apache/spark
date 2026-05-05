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
package org.apache.spark.udf.worker.core

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Minimal logging surface used by the udf/worker framework.
 *
 * The framework deliberately does not depend on SLF4J (or any other
 * concrete logging backend) so callers can embed it without dragging a
 * specific logger onto the classpath. Embedders should supply an
 * adapter that forwards to their preferred backend (Spark's `Logging`
 * trait, SLF4J, java.util.logging, etc.).
 *
 * Only the methods actually used by the framework are exposed.
 * Messages are passed by-name so the formatting cost is avoided when
 * the backend decides to drop the event.
 */
@Experimental
trait WorkerLogger {
  def warn(msg: => String): Unit
  def warn(msg: => String, t: Throwable): Unit
  def info(msg: => String): Unit
  def info(msg: => String, t: Throwable): Unit
  def debug(msg: => String): Unit
  def debug(msg: => String, t: Throwable): Unit

  /**
   * Returns a new [[WorkerLogger]] that prefixes every message with
   * `[className]`. Useful for identifying which class produced a
   * log line.
   */
  def forClass(clazz: Class[_]): WorkerLogger = {
    val prefix = s"[${clazz.getSimpleName}] "
    val parent = this
    new WorkerLogger {
      override def warn(msg: => String): Unit =
        parent.warn(prefix + msg)
      override def warn(msg: => String, t: Throwable): Unit =
        parent.warn(prefix + msg, t)
      override def info(msg: => String): Unit =
        parent.info(prefix + msg)
      override def info(msg: => String, t: Throwable): Unit =
        parent.info(prefix + msg, t)
      override def debug(msg: => String): Unit =
        parent.debug(prefix + msg)
      override def debug(msg: => String, t: Throwable): Unit =
        parent.debug(prefix + msg, t)
    }
  }
}

object WorkerLogger {
  /** Discards all messages. Default for callers that don't wire up logging. */
  val NoOp: WorkerLogger = new WorkerLogger {
    override def warn(msg: => String): Unit = ()
    override def warn(msg: => String, t: Throwable): Unit = ()
    override def info(msg: => String): Unit = ()
    override def info(msg: => String, t: Throwable): Unit = ()
    override def debug(msg: => String): Unit = ()
    override def debug(msg: => String, t: Throwable): Unit = ()
  }
}
