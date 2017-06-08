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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import com.codahale.metrics.Timer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils




// One producer one consumer asynchronous queue.
private[spark] abstract class ListenerBusQueue (
  busName: String,
  bufferSize: Int,
  withEventProcessingTime: Boolean,
  private val eventFilter: SparkListenerEvent => Boolean) extends Logging {

  import ListenerBusQueue._

  private var sparkContext: SparkContext = _

  private val circularBuffer = new Array[SparkListenerEvent](bufferSize)
  private val numberOfEvents = new AtomicInteger(0)

  @volatile private var writeIndex = 0
  @volatile private var readIndex = 0

  @volatile private var hasDropped = false
  @volatile private var numberOfDrop = 0

  private val stopped = new AtomicBoolean(false)

  private[scheduler] val metrics =
    new QueueMetrics(busName, circularBuffer, withEventProcessingTime)

  private val consumerThread = new Thread(s"$busName bus consumer") {
    setDaemon(true)
    override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
      LiveListenerBus.withinListenerThread.withValue(true) {
        val oTimer = metrics.eventProcessingTime
        while (!stopped.get() || numberOfEvents.get() > 0) {
          if (numberOfEvents.get() > 0) {
            val timerContext = oTimer.map(_.time())
            try {
              consumeEvent(circularBuffer(readIndex))
            } catch {
              case NonFatal(e) =>
                logError(s"Listener bus $busName threw an exception", e)
            }
            timerContext.foreach(_.stop())
            numberOfEvents.decrementAndGet()
            readIndex = (readIndex + 1) % bufferSize
          } else {
            Thread.sleep(20) // give more chance for producer thread to be scheduled
          }
        }
      }
    }
  }

  // should be called only once
  private[scheduler] def start(sc: SparkContext, metricsSystem: MetricsSystem): Unit = {
    sparkContext = sc
    metricsSystem.registerSource(metrics)
    initAdditionalMetrics(metrics)
    consumerThread.start()
  }

  private[scheduler] def askStop(): Unit = stopped.set(true)

  // should be called only once
  private[scheduler] def waitForStop(): Unit = {
    if (!stopped.get()) {
      throw new IllegalStateException(s"$busName was not asked for stop !")
    }
    consumerThread.join()
  }

  private[scheduler] def post(event: SparkListenerEvent): Unit = {
    if (eventFilter(event)) {
      if (numberOfEvents.get() < bufferSize) {
        circularBuffer(writeIndex) = event
        numberOfEvents.incrementAndGet()
        writeIndex = (writeIndex + 1) % bufferSize
        metrics.numEventsPosted.inc()
      } else {
        onDropEvent()
      }
    }
  }

  // For test only
  private[scheduler] def isAlive: Boolean = consumerThread.isAlive

  // For test only
  private[scheduler] def isQueueEmpty: Boolean = circularBuffer.isEmpty


  private def onDropEvent(): Unit = {
    if (!hasDropped) {
      hasDropped = true
      logError(s"Dropping SparkListenerEvent from the bus $busName because no remaining " +
        "room in event queue. " +
        "This likely means one of the SparkListeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
    numberOfDrop = numberOfDrop + 1
    metrics.numDroppedEvents.inc()
    if (numberOfDrop == DROP_MESSAGE_LOG_FREQUENCY) {
      logWarning(s"$DROP_MESSAGE_LOG_FREQUENCY SparkListenerEvents have been dropped " +
        s"from the bus $busName")
      numberOfDrop = 0
    }
  }

  protected def consumeEvent(ev: SparkListenerEvent): Unit

  private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag]: Seq[T]

  private[spark] def listeners: Seq[SparkListenerInterface]

  protected def initAdditionalMetrics(queueMetrics: QueueMetrics): Unit = {}
}

private[scheduler] object ListenerBusQueue {

  private val DROP_MESSAGE_LOG_FREQUENCY = 50
  private[scheduler] val ALL_MESSAGES: SparkListenerEvent => Boolean = _ => true

  private[bus] abstract class GroupSparkListener() extends SparkListenerInterface {

    private[bus] def listeners: Seq[(SparkListenerInterface, Option[Timer])]

    private[bus] def busName: String

    override def onStageCompleted(
      stageCompleted: SparkListenerStageCompleted): Unit =
      postToAll(stageCompleted, _.onStageCompleted)

    override def onStageSubmitted(
      stageSubmitted: SparkListenerStageSubmitted): Unit =
      postToAll(stageSubmitted, _.onStageSubmitted)

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit =
      postToAll(taskStart, _.onTaskStart)

    override def onTaskGettingResult(
      taskGettingResult: SparkListenerTaskGettingResult): Unit =
      postToAll(taskGettingResult, _.onTaskGettingResult)

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
    postToAll(taskEnd, _.onTaskEnd)

    override def onJobStart(jobStart: SparkListenerJobStart): Unit =
    postToAll(jobStart, _.onJobStart)

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit =
    postToAll(jobEnd, _.onJobEnd)

    override def onEnvironmentUpdate(
      environmentUpdate: SparkListenerEnvironmentUpdate): Unit =
    postToAll(environmentUpdate, _.onEnvironmentUpdate)

    override def onBlockManagerAdded(
      blockManagerAdded: SparkListenerBlockManagerAdded): Unit =
    postToAll(blockManagerAdded, _.onBlockManagerAdded)

    override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit =
    postToAll(blockManagerRemoved, _.onBlockManagerRemoved)

    override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit =
    postToAll(unpersistRDD, _.onUnpersistRDD)

    override def onApplicationStart(
      applicationStart: SparkListenerApplicationStart): Unit =
    postToAll(applicationStart, _.onApplicationStart)

    override def onApplicationEnd(
      applicationEnd: SparkListenerApplicationEnd): Unit =
    postToAll(applicationEnd, _.onApplicationEnd)

    override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit =
    postToAll(executorMetricsUpdate, _.onExecutorMetricsUpdate)

    override def onExecutorAdded(
      executorAdded: SparkListenerExecutorAdded): Unit =
    postToAll(executorAdded, _.onExecutorAdded)

    override def onExecutorRemoved(
      executorRemoved: SparkListenerExecutorRemoved): Unit =
    postToAll(executorRemoved, _.onExecutorRemoved)

    override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit =
    postToAll(executorBlacklisted, _.onExecutorBlacklisted)

    override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit =
    postToAll(executorUnblacklisted, _.onExecutorUnblacklisted)

    override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit =
    postToAll(nodeBlacklisted, _.onNodeBlacklisted)

    override def onNodeUnblacklisted(
      nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit =
    postToAll(nodeUnblacklisted, _.onNodeUnblacklisted)

    override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit =
    postToAll(blockUpdated, _.onBlockUpdated)

    override def onOtherEvent(event: SparkListenerEvent): Unit =
    postToAll(event, _.onOtherEvent)

    private def postToAll[T <: SparkListenerEvent](ev: T,
      func: SparkListenerInterface => T => Unit): Unit = {
      val currentCollection = listeners
      var i = 0
      while (i < currentCollection.length) {
        val listenerAndTimer = currentCollection(i)
        val timer = listenerAndTimer._2.map(_.time())
        func(listenerAndTimer._1)(ev)
        timer.foreach(_.stop())
        i = i + 1
      }
    }
  }

  private[scheduler] class FixGroupOfListener(
    listenerSeq: Seq[SparkListenerInterface],
    override val busName: String)
    extends GroupSparkListener {

    private var listenerWithTimer: Seq[(SparkListenerInterface, Option[Timer])] =
      listenerSeq.map(l => (l, None))

    override private[bus] def listeners = listenerWithTimer

    private[bus] def initTimers(metrics: QueueMetrics): Unit = {
      listenerWithTimer = listenerSeq.map(l =>
        (l, Some(metrics.getTimerForIndividualListener(l.getClass.getSimpleName))))
    }
  }

  private[bus] class ModifiableGroupOfListener(override val busName: String)
    extends GroupSparkListener {

    private var queueMetrics: Option[QueueMetrics] = None

    private val group: AtomicReference[Seq[(SparkListenerInterface, Option[Timer])]] =
      new AtomicReference[Seq[(SparkListenerInterface, Option[Timer])]](Seq.empty)

    override private[bus] def listeners = group.get()

    private[bus] def initTimers(metrics: QueueMetrics): Unit = {
      queueMetrics = Some(metrics)
      val current = listeners
      group.set(
        current.map(l => (l._1,
          Some(metrics.getTimerForIndividualListener(l._1.getClass.getSimpleName)))))
    }

    private[scheduler] def addListener(l: SparkListenerInterface): Unit = {
      val current = listeners
      val newVal = current :+ (l,
        queueMetrics.map(_.getTimerForIndividualListener(l.getClass.getSimpleName)))
      group.set(newVal)
    }

    private[scheduler] def removeListener(l: SparkListenerInterface): Unit = {
      val current = listeners
      val newVal = current.filter(t => !(t._1 == l))
      group.set(newVal)
    }

  }

}
