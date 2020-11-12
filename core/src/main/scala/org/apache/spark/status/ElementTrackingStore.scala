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

package org.apache.spark.status

import java.util.Collection
import java.util.concurrent.{ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status._
import org.apache.spark.status.ElementTrackingStore._
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.kvstore._

/**
 * A KVStore wrapper that allows tracking the number of elements of specific types, and triggering
 * actions once they reach a threshold. This allows writers, for example, to control how much data
 * is stored by potentially deleting old data as new data is added.
 *
 * This store is used when populating data either from a live UI or an event log. On top of firing
 * triggers when elements reach a certain threshold, it provides two extra bits of functionality:
 *
 * - a generic worker thread that can be used to run expensive tasks asynchronously; the tasks can
 *   be configured to run on the calling thread when more determinism is desired (e.g. unit tests).
 * - a generic flush mechanism so that listeners can be notified about when they should flush
 *   internal state to the store (e.g. after the SHS finishes parsing an event log).
 *
 * The configured triggers are run on a separate thread by default; they can be forced to run on
 * the calling thread by setting the `ASYNC_TRACKING_ENABLED` configuration to `false`.
 */
private[spark] class ElementTrackingStore(store: KVStore, conf: SparkConf) extends KVStore {

  private class LatchedTriggers(val triggers: Seq[Trigger[_]]) {
    private val pending = new AtomicBoolean(false)

    def fireOnce(f: Seq[Trigger[_]] => Unit): WriteQueueResult = {
      if (pending.compareAndSet(false, true)) {
        doAsync {
          pending.set(false)
          f(triggers)
        }
        WriteQueued
      } else {
        WriteSkippedQueue
      }
    }

    def :+(addlTrigger: Trigger[_]): LatchedTriggers = {
      new LatchedTriggers(triggers :+ addlTrigger)
    }
  }

  private val triggers = new HashMap[Class[_], LatchedTriggers]()
  private val flushTriggers = new ListBuffer[() => Unit]()
  private val executor: ExecutorService = if (conf.get(ASYNC_TRACKING_ENABLED)) {
    ThreadUtils.newDaemonSingleThreadExecutor("element-tracking-store-worker")
  } else {
    ThreadUtils.sameThreadExecutorService
  }

  @volatile private var stopped = false

  /**
   * Register a trigger that will be fired once the number of elements of a given type reaches
   * the given threshold.
   *
   * @param klass The type to monitor.
   * @param threshold The number of elements that should trigger the action.
   * @param action Action to run when the threshold is reached; takes as a parameter the number
   *               of elements of the registered type currently known to be in the store.
   */
  def addTrigger(klass: Class[_], threshold: Long)(action: Long => Unit): Unit = {
    val newTrigger = Trigger(threshold, action)
    triggers.get(klass) match {
      case None =>
        triggers(klass) = new LatchedTriggers(Seq(newTrigger))
      case Some(latchedTrigger) =>
        triggers(klass) = latchedTrigger :+ newTrigger
    }
  }

  /**
   * Adds a trigger to be executed before the store is flushed. This normally happens before
   * closing, and is useful for flushing intermediate state to the store, e.g. when replaying
   * in-progress applications through the SHS.
   *
   * Flush triggers are called synchronously in the same thread that is closing the store.
   */
  def onFlush(action: => Unit): Unit = {
    flushTriggers += { () => action }
  }

  /**
   * Enqueues an action to be executed asynchronously. The task will run on the calling thread if
   * `ASYNC_TRACKING_ENABLED` is `false`.
   */
  def doAsync(fn: => Unit): Unit = {
    executor.submit(new Runnable() {
      override def run(): Unit = Utils.tryLog { fn }
    })
  }

  override def read[T](klass: Class[T], naturalKey: Any): T = store.read(klass, naturalKey)

  override def write(value: Any): Unit = store.write(value)

  /** Write an element to the store, optionally checking for whether to fire triggers. */
  def write(value: Any, checkTriggers: Boolean): WriteQueueResult = {
    write(value)

    if (checkTriggers && !stopped) {
      triggers.get(value.getClass).map { latchedList =>
        latchedList.fireOnce { list =>
          val count = store.count(value.getClass)
          list.foreach { t =>
            if (count > t.threshold) {
              t.action(count)
            }
          }
        }
      }.getOrElse(WriteSkippedQueue)
    } else {
      WriteSkippedQueue
    }
  }

  def removeAllByIndexValues[T](klass: Class[T], index: String, indexValues: Iterable[_]): Boolean =
    removeAllByIndexValues(klass, index, indexValues.asJavaCollection)

  override def removeAllByIndexValues[T](
      klass: Class[T],
      index: String,
      indexValues: Collection[_]): Boolean = {
    store.removeAllByIndexValues(klass, index, indexValues)
  }

  override def delete(klass: Class[_], naturalKey: Any): Unit = store.delete(klass, naturalKey)

  override def getMetadata[T](klass: Class[T]): T = store.getMetadata(klass)

  override def setMetadata(value: Any): Unit = store.setMetadata(value)

  override def view[T](klass: Class[T]): KVStoreView[T] = store.view(klass)

  override def count(klass: Class[_]): Long = store.count(klass)

  override def count(klass: Class[_], index: String, indexedValue: Any): Long = {
    store.count(klass, index, indexedValue)
  }

  override def close(): Unit = {
    close(true)
  }

  /** A close() method that optionally leaves the parent store open. */
  def close(closeParent: Boolean): Unit = synchronized {
    if (stopped) {
      return
    }

    stopped = true
    executor.shutdown()
    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
      executor.shutdownNow()
    }

    flushTriggers.foreach { trigger =>
      Utils.tryLog(trigger())
    }

    if (closeParent) {
      store.close()
    }
  }

  private case class Trigger[T](
      threshold: Long,
      action: Long => Unit)

}

private[spark] object ElementTrackingStore {
  /**
   * This trait is solely to assist testing the correctness of single-fire execution
   * The result of write() is otherwise unused.
   */
  sealed trait WriteQueueResult

  object WriteQueued extends WriteQueueResult
  object WriteSkippedQueue extends WriteQueueResult
}
