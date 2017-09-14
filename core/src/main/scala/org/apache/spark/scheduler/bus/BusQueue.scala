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

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import com.codahale.metrics.Timer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.bus.BusQueue.SparkListenerEventProcessor
import org.apache.spark.util.Utils


// One producer one consumer asynchronous queue.
private[spark] class BusQueue (
  val processor: SparkListenerEventProcessor,
  bufferSize: Int,
  withEventProcessingTime: Boolean,
  private val eventFilter: SparkListenerEvent => Boolean) extends Logging {

  import BusQueue._

  private var sparkContext: SparkContext = _

  private val circularBuffer = new ArrayBlockingQueue[SparkListenerEvent](bufferSize)
  private val numberOfEvents = new AtomicInteger(0)

  private val hasDropped = new AtomicBoolean(false)
  private val numberOfDrop = new AtomicLong(0L)

  private val stopped = new AtomicBoolean(false)

  private[scheduler] val metrics =
    new QueueMetrics(processor.name, numberOfEvents, withEventProcessingTime)

  private val consumerThread = new Thread(s"${processor.name} bus consumer") {
    setDaemon(true)
    override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
      LiveListenerBus.withinListenerThread.withValue(true) {
        val oTimer = metrics.eventProcessingTime
        while (true) {
          val newElem = circularBuffer.take()
          if (newElem.eq(LAST_PROCESSED_MESSAGE)) {
            return
          }
          else {
            val timerContext = oTimer.map(_.time())
            try {
              consumeEvent(newElem)
            } catch {
              case NonFatal(e) =>
                logError(s"Listener bus ${processor.name} threw an exception", e)
            }
            numberOfEvents.decrementAndGet()
            timerContext.foreach(_.stop())
          }
        }
      }
    }
  }

  // should be called only once
  private[scheduler] def start(sc: SparkContext, metricsSystem: MetricsSystem): Unit = {
    sparkContext = sc
    initAdditionalMetrics(metrics)
    metricsSystem.registerSource(metrics)
    consumerThread.start()
  }

  private[scheduler] def askStop(): Unit = stopped.set(true)

  // should be called only once
  private[scheduler] def waitForStop(): Unit = {
    if (!stopped.get()) {
      throw new IllegalStateException(s"${processor.name} was not asked for stop !")
    }
    circularBuffer.put(LAST_PROCESSED_MESSAGE)
    consumerThread.join()
  }

  private[scheduler] def post(event: SparkListenerEvent): Unit = {
    if (eventFilter(event)) {
      val hasPosted = circularBuffer.offer(event)
      if (hasPosted) {
        numberOfEvents.incrementAndGet()
        metrics.numEventsPosted.inc()
      } else {
        onDropEvent()
      }
    }
  }

  // For test only
  private[scheduler] def isAlive: Boolean = consumerThread.isAlive

  // For test only
  // need to test both value to be sure that the queue is empty and no event is being processed
  private[scheduler] def isQueueEmpty: Boolean =
    circularBuffer.size() == 0 && numberOfEvents.get() == 0


  private def onDropEvent(): Unit = {
    if (hasDropped.compareAndSet(false, true)) {
      logError(s"Dropping SparkListenerEvent from the bus ${processor.name} because no remaining " +
        "room in event queue. " +
        "This likely means one of the SparkListeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
    numberOfDrop.incrementAndGet()
    metrics.numDroppedEvents.inc()
    if (numberOfDrop.get() >= DROP_MESSAGE_LOG_FREQUENCY) {
      logWarning(s"$DROP_MESSAGE_LOG_FREQUENCY SparkListenerEvents have been dropped " +
        s"from the bus ${processor.name}")
      numberOfDrop.set(0L)
    }
  }

  private def consumeEvent(ev: SparkListenerEvent): Unit = processor.consumeEvent(ev)

  private[spark] def listeners: Seq[SparkListenerInterface] =
    processor.oListener
      .map {
        case group: GroupOfListener => group.listeners.map(_._1)
        case l => Seq(l)
      }
    .getOrElse(Seq.empty)

  protected def initAdditionalMetrics(queueMetrics: QueueMetrics): Unit =
    processor.oListener
      .foreach { case group: GroupOfListener => group.initTimers(queueMetrics) }

  private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.filter(_.getClass == c).map(_.asInstanceOf[T])
  }

}

private[spark] object BusQueue {

  private val DROP_MESSAGE_LOG_FREQUENCY = 50

  private object LAST_PROCESSED_MESSAGE extends SparkListenerEvent

  def apply(busName: String,
            bufferSize: Int,
            process: SparkListenerEvent => Unit,
            eventFilter: SparkListenerEvent => Boolean): BusQueue =
    new BusQueue(GenericProcessor(process, busName), bufferSize, true, eventFilter)

  def apply(bufferSize: Int,
            listener: SparkListenerInterface,
            eventFilter: SparkListenerEvent => Boolean): BusQueue =
    new BusQueue(
      ListenerProcessor(listener),
      bufferSize,
      listener match {
        case _: GroupOfListener => false
        case _ => true
      },
      eventFilter
    )

  private[scheduler] trait SparkListenerEventProcessor {
    def consumeEvent(ev: SparkListenerEvent): Unit

    val oListener: Option[SparkListenerInterface]

    val name: String
  }

  private case class ListenerProcessor(listener: SparkListenerInterface)
    extends SparkListenerEventProcessor{
    override def consumeEvent(ev: SparkListenerEvent): Unit =
      SparkListenerEventDispatcher.dispatch(listener, ev)

    override val oListener: Option[SparkListenerInterface] = Some(listener)
    override val name: String = listener match {
      case group: GroupOfListener => group.busName
      case simpleListener => simpleListener.getClass.getSimpleName
    }
  }

  private[scheduler] case class GenericProcessor(process: SparkListenerEvent => Unit, label: String)
    extends SparkListenerEventProcessor{
    override def consumeEvent(ev: SparkListenerEvent): Unit = process(ev)

    override val oListener: Option[SparkListenerInterface] = None
    override val name: String = label
  }

  private[scheduler] val ALL_MESSAGES: SparkListenerEvent => Boolean = _ => true

  private[spark] object GroupOfListener {

    def apply(listenerSeq: Seq[SparkListenerInterface], busName: String): GroupOfListener = {
      val group = GroupOfListener(busName)
      listenerSeq.foreach( l => group.addListener(l))
      group
    }

    def apply(busName: String): GroupOfListener = {
      GroupOfListener(busName)
    }
  }

  private[bus] class GroupOfListener(val busName: String)
    extends SparkListenerInterface with Logging {

    private var queueMetrics: Option[QueueMetrics] = None

    private val group: AtomicReference[Seq[(SparkListenerInterface, Option[Timer])]] =
      new AtomicReference[Seq[(SparkListenerInterface, Option[Timer])]](Seq.empty)

    private[scheduler] def listeners = group.get()

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
        try {
          func(listenerAndTimer._1)(ev)
        } catch {
          case NonFatal(e) =>
            logError(s"a listener ${listenerAndTimer._1.getClass.getName} threw an exception", e)
        }
        timer.foreach(_.stop())
        i = i + 1
      }
    }
  }

}
