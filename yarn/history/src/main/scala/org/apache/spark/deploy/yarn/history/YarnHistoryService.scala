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

package org.apache.spark.deploy.yarn.history

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.LinkedBlockingQueue
import java.util.{HashMap => JHashMap}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.service.AbstractService
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity,
  TimelineEvent, TimelineDomain, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.deploy.yarn.history.TimestampEvent
import org.apache.spark.scheduler.cluster.YarnService
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}
import org.apache.spark.{Logging, SparkContext}
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.LinkedList

import scala.collection.JavaConversions._

class YarnHistoryService  extends AbstractService("ATS")
  with YarnService with Logging {

  private var sc: SparkContext = _
  private var appId: ApplicationId = _
  private var timelineClient: Option[TimelineClient] = None
  private var listener: YarnEventListener = _
  private var appName: String = null
  private var userName: String = null
  private var startTime: Long = _

  private var batchSize: Int = YarnHistoryService.DEFAULT_BATCH_SIZE

  // enqueue event to avoid blocking on main thread.
  private var eventQueue = new LinkedBlockingQueue[TimestampEvent]
  // cache layer to handle timeline client failure.
  private var entityList = new LinkedList[TimelineEntity]
  private var curEntity: Option[TimelineEntity] = None
  // Do we have enough information filled for the entity
  private var bAppStart = false
  private var bAppEnd = false
  // How many event we saved
  private var curEventNum = 0
  private var eventsProcessed: Int = 0
  private var eventHandlingThread: Thread = null
  private var stopped: AtomicBoolean = new AtomicBoolean(true)
  private final val lock: AnyRef = new AnyRef
  private var maxTimeToWaitOnShutdown: Long = YarnHistoryService.DEFAULT_WAIT_TIME
  private var clientFailure = 0
  private var domainId: String = null

  def createTimelineClient = {
    clientFailure += 1
    logInfo("Creating timelineClient " + clientFailure)
    val client = TimelineClient.createTimelineClient()
    client.init(sc.hadoopConfiguration)
    client.start
    timelineClient = Some(client)
    client
  }

  def getTimelineClient = timelineClient.getOrElse(createTimelineClient)

  def stopTimelineClient = {
    timelineClient.map(_.stop)
    timelineClient = None
  }

  /**
   * Split a comma separated String, filter out any empty items, and return a Set of strings
   */
  private def stringToSet(list: String): Set[String] = {
    list.split(',').map(_.trim).filter(!_.isEmpty).toSet
  }

  private  def createTimelineDomain():String = {
    val sparkConf = sc.getConf
    val aclsOn = sparkConf.getOption("spark.acls.enable").getOrElse(
      sparkConf.get("spark.ui.acls.enable", "false")).toBoolean
    if (!aclsOn) {
      return null
    }
    val predefDomain = sparkConf.getOption("spark.ui.domain")
    if (predefDomain.isDefined) {
      domainId = predefDomain.get
      return null
    }
    val current = UserGroupInformation.getCurrentUser.getShortUserName
    val adminAcls  = stringToSet(sparkConf.get("spark.admin.acls", ""))
    val viewAcls = stringToSet(sparkConf.get("spark.ui.view.acls", ""))
    val modifyAcls = stringToSet(sparkConf.get("spark.modify.acls", ""))

    val readers = (adminAcls ++ modifyAcls ++ viewAcls).foldLeft(current)(_ + " " + _)
    val writers = (adminAcls ++ modifyAcls).foldLeft(current)(_ + " " + _)
    var tmpId = YarnHistoryService.DOMAIN_ID_PREFIX + appId
    logInfo("Creating domain " + tmpId + " with  readers: "
      + readers + " and writers:" + writers)
    val timelineDomain = new TimelineDomain();
    timelineDomain.setId(tmpId);

    timelineDomain.setReaders(readers)
    timelineDomain.setWriters(writers);
    try {
      getTimelineClient.putDomain(timelineDomain);
    } catch {
      case e: YarnException => {
        logError("cannot create the domain")
        // fallback to default
        tmpId = null
      }
    }
    tmpId
  }

  def start(context: SparkContext, id: ApplicationId): Unit = {
    // Check that the configuration points at an AHS, otherwise the client code will
    // not be able to connect.
    val yarnConf = new YarnConfiguration(context.hadoopConfiguration)
    if (!yarnConf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
      YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
      logInfo("Yarn timeline service not available, disabling client.")
      return
    }
    if (!stopped.get()) {
      return
    }
    stopped.set(false)
    sc = context
    appId = id
    addShutdownHook(this)
    init(sc.hadoopConfiguration)
    start()
    listener = new YarnEventListener(sc, this)
    sc.listenerBus.addListener(listener)
    logInfo("History service started")
  }

  override def serviceInit(conf: Configuration) {
    createTimelineClient
    domainId = createTimelineDomain
  }

  private def addShutdownHook(service: YarnHistoryService) {
    Runtime.getRuntime.addShutdownHook(new Thread("terminating logging service") {
      override def run() = {
        logInfo("Shutdown hook called")
        service.stop
      }
    })
  }

  override def serviceStart {
    eventHandlingThread = new Thread(new Runnable {
      def run {
        var event: Any = null
        log.info("Starting service for AppId " + appId)
        while (!stopped.get && !Thread.currentThread.isInterrupted) {
          try {
            event = eventQueue.take
            eventsProcessed += 1
            handleEvent(event.asInstanceOf[TimestampEvent], false)
          } catch {
            case e: Exception => {
              logWarning("EventQueue take interrupted. Returning")
            }
          }
        }
      }
    }, "HistoryEventHandlingThread")
    eventHandlingThread.start
  }

  def enqueue(event: TimestampEvent) = {
    if (!stopped.get()) {
      eventQueue.add(event)
    } else {
      logInfo("enqueue events with ATS service stopped")
    }
  }

  override def serviceStop {
    if (!stopped.getAndSet(true)) {
      if (eventHandlingThread != null) {
        eventHandlingThread.interrupt
      }
      if (!bAppEnd) {
        eventQueue.add(new TimestampEvent(SparkListenerApplicationEnd(System.currentTimeMillis()),
          System.currentTimeMillis()))
      }
      logDebug("push out all events")
      if (!eventQueue.isEmpty) {
        if (maxTimeToWaitOnShutdown > 0) {
          val curTime: Long = System.currentTimeMillis()
          val endTime: Long = curTime + maxTimeToWaitOnShutdown
          var event = eventQueue.poll
          while (endTime >= System.currentTimeMillis() && event != null) {
            handleEvent(event, true)
            event = eventQueue.poll
          }
        }
      } else {
        //flush all entities
        handleEvent(null, true)
      }
      if (!eventQueue.isEmpty) {
        logWarning("Did not finish flushing eventQueue before " +
          "stopping ATSService, eventQueueBacklog=" + eventQueue.size)
      }
      stopTimelineClient
      logInfo("ATS service terminated")
    }
  }

  def getCurrentEntity = {
    curEntity.getOrElse {
      val entity: TimelineEntity = new TimelineEntity
      logDebug("Create new entity")
      curEventNum = 0
      entity.setEntityType(YarnHistoryService.ENTITY_TYPE)
      entity.setEntityId(appId.toString)
      if (bAppStart) {
        entity.addPrimaryFilter("appName", appName)
        entity.addPrimaryFilter("appUser", userName)
        entity.addOtherInfo("appName", appName)
        entity.addOtherInfo("appUser", userName)
      }
      curEntity = Some(entity)
      entity
    }
  }

  /**
   * If there is any available entity to be sent, push to timeline server
   * @return
   */
  def flushEntity(): Unit = {
    if (entityList.isEmpty) {
      return
    }
    logDebug("before pushEntities: " + entityList.size())
    var client = getTimelineClient
    entityList = entityList.filter {
      en => {
        if (en == null) {
          false
        } else {
          if (domainId != null) {
            en.setDomainId(domainId);
          }
          try {
            val response: TimelinePutResponse = client.putEntities(en)
            if (response != null && !response.getErrors.isEmpty) {
              val err: TimelinePutResponse.TimelinePutError = response.getErrors.get(0)
              if (err.getErrorCode != 0) {
                timelineClient = None
                logError("Could not post history event to ATS, eventType=" + err.getErrorCode)
              }
              true
            } else {
              logDebug("entity pushed: " + en)
              false
            }
          } catch {
            case e: Exception => {
              timelineClient = None
              client = getTimelineClient
              logError("Could not handle history entity: " + e)
              true
            }
          }
        }
      }
    }
    logDebug("after pushEntities: " + entityList.size())
  }

  /**
   * If the event reaches the batch size or flush is true, push events to ATS.
   *
   * @param event
   * @param flush
   * @return
   */
  private def handleEvent(event: TimestampEvent,  flush: Boolean): Unit = {
    var push = false
    // if we receive a new appStart event, we always push
    // not much contention here, only happens when service is stopped
    lock synchronized {
      if (event != null) {
        if (eventsProcessed % 1000 == 0) {
          logDebug("$eventProcessed events are processed")
        }
        eventsProcessed += 1
        val obj = JsonProtocol.sparkEventToJson(event.sparkEvent)
        val map = compact(render(obj))
        if (map == null || map == "") return
        event.sparkEvent match {
          case start: SparkListenerApplicationStart =>
            // we already have all information,
            // flush it for old one to switch to new one
            logDebug("Receive application start event: " + event)
            // flush old entity
            curEntity.foreach(entityList :+= _)
            curEntity = None
            appName =start.appName;
            userName = start.sparkUser
            startTime = start.time
            val en = getCurrentEntity
            en.addPrimaryFilter("startApp", "newApp")
            push = true
            bAppStart = true
            bAppEnd = false
          case end: SparkListenerApplicationEnd =>
            if (!bAppEnd) {
              // we already have all information,
              // flush it for old one to switch to new one
              logDebug("Receive application end event: " + event)
              // flush old entity
              curEntity.foreach(entityList :+= _)
              curEntity = None

              val en = getCurrentEntity
              en.addPrimaryFilter("endApp", "oldApp")
              en.addOtherInfo("startTime", startTime)
              en.addOtherInfo("endTime", end.time)
              bAppEnd = true
              bAppStart = false
              push = true
              this.stop
            }
          case _ =>
        }
        val tlEvent = new TimelineEvent()
        tlEvent.setEventType(Utils.getFormattedClassName(event.sparkEvent).toString)
        tlEvent.setTimestamp(event.time)
        val kvMap = new JHashMap[String, Object]();
        kvMap.put(Utils.getFormattedClassName(event.sparkEvent).toString, map)
        tlEvent.setEventInfo(kvMap)
        getCurrentEntity.addEvent(tlEvent)
        curEventNum += 1
      }
      logDebug("current event num: " + curEventNum)
      if (curEventNum == batchSize || flush || push) {
        curEntity.foreach(entityList :+= _)
        curEntity = None
        curEventNum = 0
      }
      flushEntity()
    }
  }
}

object YarnHistoryService {
  val ENTITY_TYPE = "SparkApplication"
  val DOMAIN_ID_PREFIX = "Spark_ATS_"
  val DEFAULT_BATCH_SIZE = 3
  val DEFAULT_WAIT_TIME = 1000L
}
