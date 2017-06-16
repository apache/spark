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

package org.apache.spark.scheduler.bus

import com.codahale.metrics.Timer
import scala.reflect.ClassTag

import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerEventDispatcher, SparkListenerInterface}
import org.apache.spark.scheduler.bus.ListenerBusQueue.{FixGroupOfListener, ModifiableGroupOfListener}

// For generic message processor (like event logging)
private[scheduler] class ProcessorListenerBusQueue(
  busName: String,
  bufferSize: Int,
  process: SparkListenerEvent => Unit,
  eventFilter: SparkListenerEvent => Boolean
) extends ListenerBusQueue(busName, bufferSize, withEventProcessingTime = true, eventFilter) {

  override protected def consumeEvent(ev: SparkListenerEvent): Unit = {
    process(ev)
  }

  override private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag] =
    Seq.empty[T]

  override private[spark] val listeners = Seq.empty
}

// Useful for isolated listener (like ExecutorAllocationListener)
// or for group of immutable isolated listeners (like UI listeners)
private[scheduler] class SingleListenerBusQueue(
  bufferSize: Int,
  listener: SparkListenerInterface,
  eventFilter: SparkListenerEvent => Boolean)
  extends ListenerBusQueue(
    listener match {
      case group: FixGroupOfListener => group.busName
      case anyListener => anyListener.getClass.getSimpleName
    },
    bufferSize,
    listener match {
      case _: FixGroupOfListener => false
      case _ => true
    },
    eventFilter) {

  override protected def consumeEvent(ev: SparkListenerEvent): Unit = {
    SparkListenerEventDispatcher.dispatch(listener, ev)
  }

  override private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.filter(_.getClass == c).map(_.asInstanceOf[T])
  }

  override private[spark] val listeners = {
    listener match {
      case group: FixGroupOfListener =>
        group.listeners.map(_._1)
      case _ =>
        Seq(listener)
    }
  }

  override protected def initAdditionalMetrics(queueMetrics: QueueMetrics): Unit = {
    listener match {
      case group: FixGroupOfListener => group.initTimers(queueMetrics)
      case _ =>
    }
  }
}

// Useful for the default group of listener
private[scheduler] class GroupOfListenersBusQueue(
  busName: String,
  bufferSize: Int) extends ListenerBusQueue(
  busName,
  bufferSize,
  withEventProcessingTime = false,
  ListenerBusQueue.ALL_MESSAGES) {

  private[scheduler] val groupOfListener = new ModifiableGroupOfListener(busName)

  override protected def consumeEvent(ev: SparkListenerEvent): Unit = {
    SparkListenerEventDispatcher.dispatch(groupOfListener, ev)
  }

  override private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.filter(_.getClass == c).map(_.asInstanceOf[T])
  }

  override private[spark] def listeners = groupOfListener.listeners.map(_._1)

  override protected def initAdditionalMetrics(queueMetrics: QueueMetrics): Unit = {
    groupOfListener.initTimers(metrics)
  }

   /**
    * Exposed for testing.
    */
  private[scheduler] def listenerWithCounter: Map[SparkListenerInterface, Option[Timer]] = {
    groupOfListener.listeners.toMap
  }

}

