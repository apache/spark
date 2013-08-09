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

package spark.scheduler

import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.LinkedBlockingQueue

/** Asynchronously passes SparkListenerEvents to registered SparkListeners. */
class SparkListenerEventProcessor() {
  /* sparkListeners is not thread safe, so this assumes that listeners are all added before any
   * SparkListenerEvents occur. */
  private val sparkListeners = ArrayBuffer[SparkListener]()
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvents]

  new Thread("SparkListenerEventProcessor") {
    setDaemon(true)
    override def run() {
      while (true) {
        val event = eventQueue.take
        event match {
          case stageSubmitted: SparkListenerStageSubmitted =>
            sparkListeners.foreach(_.onStageSubmitted(stageSubmitted))
          case stageCompleted: StageCompleted =>
            sparkListeners.foreach(_.onStageCompleted(stageCompleted))
          case jobStart: SparkListenerJobStart =>
            sparkListeners.foreach(_.onJobStart(jobStart))
          case jobEnd: SparkListenerJobEnd =>
            sparkListeners.foreach(_.onJobEnd(jobEnd))
          case taskStart: SparkListenerTaskStart =>
            sparkListeners.foreach(_.onTaskStart(taskStart))
          case taskEnd: SparkListenerTaskEnd =>
            sparkListeners.foreach(_.onTaskEnd(taskEnd))
          case _ =>
        }
      }
    }
  }.start()

  def addListener(listener: SparkListener) {
    sparkListeners += listener
  }

  def addEvent(event: SparkListenerEvents) {
    eventQueue.put(event)
  }
}
