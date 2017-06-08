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

import com.codahale.metrics.{Counter, Gauge, MetricRegistry, Timer}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.Source
import org.apache.spark.scheduler.SparkListenerEvent

private[spark] class QueueMetrics(
  busName: String,
  queue: Array[SparkListenerEvent],
  withEventProcessingTime: Boolean) extends Source with Logging {

  override val sourceName: String = s"${busName}Bus"
  override val metricRegistry: MetricRegistry = new MetricRegistry

   /**
    * The total number of events posted to the LiveListenerBus. This is a count of the total number
    * of events which have been produced by the application and sent to the listener bus, NOT a
    * count of the number of events which have been processed and delivered to listeners (or dropped
    * without being delivered).
    */
  val numEventsPosted: Counter = metricRegistry.counter(MetricRegistry.name("numEventsPosted"))

   /**
    * The total number of events that were dropped without being delivered to listeners.
    */
  val numDroppedEvents: Counter = metricRegistry.counter(MetricRegistry.name("numEventsDropped"))

   /**
    * The number of messages waiting in the queue.
    */
  val queueSize: Gauge[Int] = {
    metricRegistry.register(MetricRegistry.name("queueSize"), new Gauge[Int]{
      override def getValue: Int = queue.length
    })
  }

  val eventProcessingTime: Option[Timer] = {
    if (withEventProcessingTime) {
      Some(metricRegistry.timer(MetricRegistry.name("eventProcessingTime")))
    } else {
      None
    }
  }

  def getTimerForIndividualListener(listenerlabel: String): Timer = {
    metricRegistry.timer(MetricRegistry.name(
      "listeners",
      listenerlabel,
      "eventProcessingTime"))
  }

}

