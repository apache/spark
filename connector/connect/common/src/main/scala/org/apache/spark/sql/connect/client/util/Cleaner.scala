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
package org.apache.spark.sql.connect.client.util

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Helper class for cleaning up an object's resources after the object itself has been garbage
 * collected.
 *
 * When we move to Java 9+ we should replace this class by [[java.lang.ref.Cleaner]].
 */
private[sql] class Cleaner {
  class Ref(pin: AnyRef, val resource: AutoCloseable)
      extends WeakReference[AnyRef](pin, referenceQueue)
      with AutoCloseable {
    override def close(): Unit = resource.close()
  }

  def register(pin: Cleanable): Unit = {
    register(pin, pin.cleaner)
  }

  /**
   * Register an objects' resources for clean-up. Note that it is absolutely pivotal that resource
   * itself does not contain any reference to the object, if it does the object will never be
   * garbage collected and the clean-up will never be performed.
   *
   * @param pin
   *   who's resources need to be cleaned up after GC.
   * @param resource
   *   to clean-up.
   */
  def register(pin: AnyRef, resource: AutoCloseable): Unit = {
    referenceBuffer.add(new Ref(pin, resource))
  }

  @volatile private var stopped = false
  private val referenceBuffer = Collections.newSetFromMap[Ref](new ConcurrentHashMap)
  private val referenceQueue = new ReferenceQueue[AnyRef]

  private val cleanerThread = {
    val thread = new Thread(() => cleanUp())
    thread.setName("cleaner")
    thread.setDaemon(true)
    thread
  }

  def start(): Unit = {
    require(!stopped)
    cleanerThread.start()
  }

  def stop(): Unit = {
    stopped = true
    cleanerThread.interrupt()
  }

  private def cleanUp(): Unit = {
    while (!stopped) {
      try {
        val ref = referenceQueue.remove().asInstanceOf[Ref]
        referenceBuffer.remove(ref)
        ref.close()
      } catch {
        case NonFatal(e) =>
          // Perhaps log this?
          e.printStackTrace()
      }
    }
  }
}

trait Cleanable {
  def cleaner: AutoCloseable
}

object AutoCloseables {
  def apply(resources: Seq[AutoCloseable]): AutoCloseable = { () =>
    val throwables = mutable.Buffer.empty[Throwable]
    resources.foreach { resource =>
      try {
        resource.close()
      } catch {
        case NonFatal(e) => throwables += e
      }
    }
    if (throwables.nonEmpty) {
      val t = throwables.head
      throwables.tail.foreach(t.addSuppressed)
      throw t
    }
  }
}
