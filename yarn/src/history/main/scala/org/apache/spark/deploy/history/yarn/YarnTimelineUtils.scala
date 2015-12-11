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

package org.apache.spark.deploy.history.yarn

import java.{lang, util}
import java.io.IOException
import java.net.{InetSocketAddress, NoRouteToHostException, URI, URL}
import java.text.DateFormat
import java.util.{ArrayList => JArrayList, Collection => JCollection, Date, HashMap => JHashMap, Map => JMap}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.json4s.{MappingException, JValue}
import org.json4s.JsonAST.{JNull, JNothing, JArray, JBool, JDecimal, JDouble, JString, JInt, JObject}
import org.json4s.jackson.JsonMethods._

import org.apache.spark
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.deploy.history.yarn.YarnHistoryService._
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEvent, SparkListenerExecutorAdded, SparkListenerExecutorRemoved, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * Utility methods for timeline classes.
 */
private[spark] object YarnTimelineUtils extends Logging {

  /**
   * What attempt ID to use as the attempt ID field (not the entity ID) when
   * there is no attempt info.
   */
  val SINGLE_ATTEMPT = "1"

  /**
   * Exception text when there is no event info data to unmarshall.
   */
  val E_NO_EVENTINFO = "No 'eventinfo' entry"

  /**
   * Exception text when there is event info entry in the timeline event, but it is empty.
   */

  val E_EMPTY_EVENTINFO = "Empty 'eventinfo' entry"

  /**
   * Counter incremented on every spark event to timeline event creation,
   * so guaranteeing uniqueness of event IDs across a single application attempt
   * (which is implicitly, one per JVM).
   */
  val eventCreateCounter = new AtomicLong(System.currentTimeMillis())

  /**
   * A counter incremented every time a new entity is created. This is included as an "other"
   * field in the entity information -so can be used as a probe to determine if the entity
   * has been updated since a previous check.
   */
  val entityVersionCounter = new AtomicLong(1)

  /**
   * Converts a Java object to its equivalent json4s representation.
   */
  def toJValue(obj: Object): JValue = {
    obj match {
      case str: String => JString(str)
      case dbl: java.lang.Double => JDouble(dbl)
      case dec: java.math.BigDecimal => JDecimal(dec)
      case int: java.lang.Integer => JInt(BigInt(int))
      case long: java.lang.Long => JInt(BigInt(long))
      case bool: java.lang.Boolean => JBool(bool)
      case map: JMap[_, _] =>
        val jmap = map.asInstanceOf[JMap[String, Object]]
        JObject(jmap.entrySet().asScala.map { e => e.getKey -> toJValue(e.getValue) }.toList)
      case array: JCollection[_] =>
        JArray(array.asInstanceOf[JCollection[Object]].asScala.map(o => toJValue(o)).toList)
      case null => JNothing
    }
  }

  /**
   * Converts a JValue into its Java equivalent.
   */
  def toJavaObject(v: JValue): Object = {
    v match {
      case JNothing => null
      case JNull => null
      case JString(s) => s
      case JDouble(num) => java.lang.Double.valueOf(num)
      case JDecimal(num) => num.bigDecimal
      case JInt(num) => java.lang.Long.valueOf(num.longValue())
      case JBool(value) => java.lang.Boolean.valueOf(value)
      case obj: JObject => toJavaMap(obj)
      case JArray(vals) =>
        val list = new JArrayList[Object]()
        vals.foreach(x => list.add(toJavaObject(x)))
        list
    }
  }

  /**
   * Converts a json4s list of fields into a Java Map suitable for serialization by Jackson,
   * which is used by the ATS client library.
   */
  def toJavaMap(sourceObj: JObject): JHashMap[String, Object] = {
    val map = new JHashMap[String, Object]()
    sourceObj.obj.foreach { case (k, v) => map.put(k, toJavaObject(v)) }
    map
  }

  /**
   * Convert a timeline event to a spark one. Includes some basic checks for validity of
   * the event payload.
   * @param event timeline event
   * @return an unmarshalled event
   */
  def toSparkEvent(event: TimelineEvent): SparkListenerEvent = {
    val info = event.getEventInfo
    if (info == null) {
      throw new IOException(E_NO_EVENTINFO)
    }
    if (info.size() == 0) {
      throw new IOException(E_EMPTY_EVENTINFO)
    }
    val payload = toJValue(info)
    def jsonToString: String = {
      val json = compact(render(payload))
      val limit = 256
      if (json.length < limit) {
        json
      } else {
         json.substring(0, limit) + " ... }"
      }
    }
    logDebug(s"toSparkEvent payload is $jsonToString")
    val eventField = payload \ "Event"
    if (eventField == JNothing) {
      throw new IOException(s"No 'Event' entry in $jsonToString")
    }

    // now the real unmarshalling
    try {
      JsonProtocol.sparkEventFromJson(payload)
    } catch {
      // failure in the marshalling; include payload in the message
      case ex: MappingException =>
        logDebug(s"$ex while rendering $jsonToString", ex)
        throw ex
    }
  }

  /**
   * Convert a spark event to a timeline event
   * @param event handled spark event
   * @return a timeline event if it could be marshalled
   */
  def toTimelineEvent(event: SparkListenerEvent, timestamp: Long): Option[TimelineEvent] = {
    try {
      val tlEvent = new TimelineEvent()
      tlEvent.setEventType(Utils.getFormattedClassName(event)
          + "-" + eventCreateCounter.incrementAndGet.toString)
      tlEvent.setTimestamp(timestamp)
      val kvMap = new JHashMap[String, Object]()
      val json = JsonProtocol.sparkEventToJson(event)
      val jObject = json.asInstanceOf[JObject]
      // the timeline event wants a map of java objects for Jackson to serialize
      val hashMap = toJavaMap(jObject)
      tlEvent.setEventInfo(hashMap)
      Some(tlEvent)
    }
    catch {
      case e: MatchError =>
        log.debug(s"Failed to convert $event to JSON: $e", e)
        None
    }
  }

  /**
   * Describe the event for logging.
   *
   * @param event timeline event
   * @return a description
   */
  def describeEvent(event: TimelineEvent): String = {
    val sparkEventDetails = try {
      toSparkEvent(event).toString
    } catch {
      case _: MappingException =>
       "(cannot convert event details to spark exception)"
    }
    s"${event.getEventType()} @ ${new Date(event.getTimestamp())}" +
      s"\n    $sparkEventDetails"
  }

  /**
   * Create details of a timeline entity, by describing every event inside it.
   *
   * @param entity entity containing a possibly empty or null list of events
   * @return a list of event details, with a newline between each one
   */
  def eventDetails(entity: TimelineEntity): String = {
    val events = entity.getEvents
    if (events != null) {
      events.asScala.map(describeEvent).mkString("\n")
    } else {
      ""
    }
  }

  /**
   * Describe a timeline entity.
   * @param entity entity
   * @return a string description.
   */
  def describeEntity(entity: TimelineEntity): String = {
    val events: util.List[TimelineEvent] = entity.getEvents
    val eventSummary = if (events != null) {
      s"contains ${events.size()} event(s)"
    } else {
      "contains no events"
    }

    val domain = if (entity.getDomainId != null) s" Domain ${entity.getDomainId}" else ""
    val header = s"${entity.getEntityType}/${entity.getEntityId} $domain"
    try {
      events.asScala.map(describeEvent).mkString("\n")
      val otherInfo = entity.getOtherInfo.asScala.map {
        case (k, v) => s" $k ='$v': ${v.getClass};"
      }.mkString("\n")
      s"Timeline Entity " + header +
          " " + otherInfo + "\n" +
          " started: " + timeFieldToString(entity.getStartTime, "start") + "\n" +
          " " + eventSummary
    } catch {
      case e: MappingException =>
        // failure to marshall/unmarshall; downgrade
        s"Timeline Entity $header"
    }
  }

  /**
   * Convert a `java.lang.Long` reference to a string value, or, if the reference is null,
   * to text declaring that the named field is empty.
   *
   * @param time time reference
   * @param field field name for error message
   * @return a string to describe the field
   */
  def timeFieldToString(time: lang.Long, field: String): String = {
    if (time != null) {
      new Date(time).toString
    } else {
      s"no $field time"
    }
  }

  /**
   * A verbose description of the entity which contains event details and info about
   * primary/secondary keys.
   *
   * @param entity timeline entity
   * @return a verbose description of the field
   */
  def describeEntityVerbose(entity: TimelineEntity): String = {
    val header = describeEntity(entity)
    val primaryFilters = entity.getPrimaryFilters.asScala.toMap
    var filterElements = ""
    for ((k, v) <- primaryFilters) {
      filterElements = filterElements +
        " filter " + k + ": [ " + v.asScala.foldLeft("")((s, o) => s + o.toString + " ") + "]\n"
    }
    val events = eventDetails(entity)
    header + "\n" + filterElements + events
  }

  /**
   * Split a comma separated String, filter out any empty items, and return a `Set` of strings.
   */
  def stringToSet(list: String): Set[String] = {
    list.split(',').map(_.trim).filter(!_.isEmpty).toSet
  }

  /**
   * Try to get the event time off an event. Not all events have the required information.
   *
   * @param event event to process
   * @return the event time
   */
  def eventTime(event: SparkListenerEvent): Option[Long] = {
    event match {
      case evt: SparkListenerApplicationStart =>
        Some(evt.time)
      case evt: SparkListenerApplicationEnd =>
        Some(evt.time)
      case evt: SparkListenerJobStart =>
        Some(evt.time)
      case evt: SparkListenerJobEnd =>
        Some(evt.time)
      case evt: SparkListenerExecutorAdded =>
        Some(evt.time)
      case evt: SparkListenerExecutorRemoved =>
        Some(evt.time)
      case evt: SparkListenerStageSubmitted =>
        evt.stageInfo.submissionTime
      case evt: SparkListenerStageCompleted =>
        evt.stageInfo.completionTime
      case _ => None
    }
  }

  /**
   * Create and start a timeline client, using the configuration context to
   * set up the binding.
   *
   * @param sparkContext spark context
   * @return the started instance
   */
  def createTimelineClient(sparkContext: SparkContext): TimelineClient = {
    val client = TimelineClient.createTimelineClient
    client.init(sparkContext.hadoopConfiguration)
    client.start()
    client
  }

  /**
   * The path for the V1 ATS REST API.
   */
  val TIMELINE_REST_PATH = s"/ws/v1/timeline/"

  /**
   * Build the URI to the base of the timeline web application
   * from the Hadoop context.
   *
   * Raises an exception if the address cannot be determined or is considered invalid from
   * a networking perspective.
   *
   * Does not perform any checks as to whether or not the timeline service is enabled
   * @param conf configuration
   * @return the URI to the timeline service.
   */
  def getTimelineEndpoint(conf: Configuration): URI = {
    val isHttps = YarnConfiguration.useHttps(conf)
    val address = if (isHttps) {
      conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
                YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS)
    } else {
      conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
                YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS)
    }
    val protocol = if (isHttps) "https://" else "http://"
    require(address != null, s"No timeline service defined")
    validateEndpoint(URI.create(s"$protocol$address$TIMELINE_REST_PATH"))
  }

  /**
   * Create a URI to the history service. This uses the entity type of
   * [[YarnHistoryService#ENTITY_TYPE]] for spark application histories.
   * @param conf hadoop configuration to examine
   * @return
   */
  def timelineWebappUri(conf: Configuration): URI = {
    timelineWebappUri(conf, YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
  }

  /**
   * Get the URI of a path under the timeline web UI.
   *
   * @param conf configuration
   * @param subpath path under the root web UI
   * @return a URI
   */
  def timelineWebappUri(conf: Configuration, subpath: String): URI = {
    val base = getTimelineEndpoint(conf)
    new URL(base.toURL, subpath).toURI
  }

  /**
   * Check the service configuration to see if the timeline service is enabled.
   *
   * @return true if `YarnConfiguration.TIMELINE_SERVICE_ENABLED` is set.
   */
  def timelineServiceEnabled(conf: Configuration): Boolean = {
    conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
                    YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)
  }

  /**
   * Get the URI to an application under the timeline
   * (this requires the applicationID to have been used to
   * publish entities there)
   * @param timelineUri timeline URI
   * @param appId App ID (really, the entityId used to publish)
   * @return the path
   */
  def applicationURI(timelineUri: URI, appId: String): URI = {
    require(appId != null && !appId.isEmpty, "No application ID")
    require(!appId.contains("/"), s"Illegal character '/' in $appId")
    timelineUri.resolve(s"${timelineUri.getPath()}/$appId")
  }

  /**
   * Map an error code to a string. For known codes, it returns
   * a description; for others it just returns the error code.
   *
   * @param code error code
   * @return a string description for error messages
   */
  def timelineErrorCodeToString(code: Int): String = {
    code match {
      case 0 => "0: no error"
      case 1 => "No start time"
      case 2 => "IO Exception"
      case 3 => "System Filter Conflict"
      case 4 => "Access Denied"
      case 5 => "No Domain"
      case 6 => "Forbidden Relation"
      case other: Int => s"Error code $other"
    }
  }

  /**
   * Convert a timeline error response to a slightly more meaningful string.
   * @param error error
   * @return text for diagnostics
   */
  def describeError(error: TimelinePutError): String = {
    s"Entity ID=${error.getEntityId()}; Entity type=${error.getEntityType}" +
    s" Error code ${error.getErrorCode}" +
    s": ${timelineErrorCodeToString(error.getErrorCode)}"
  }

  /**
   * Describe a put response by enumerating and describing all errors.
   * (if present. A null `errors` element is handled robustly).
   *
   * @param response response to describe
   * @return text for diagnostics
   */
  def describePutResponse(response: TimelinePutResponse) : String = {
    val responseErrs = response.getErrors
    if (responseErrs != null) {
      val errors = mutable.MutableList(s"TimelinePutResponse with ${responseErrs.size()} errors")
      for (err <- responseErrs.asScala) {
        errors += describeError(err)
      }
      errors.foldLeft("")((buff, elt) => buff + "\n" + elt)
    } else {
      s"TimelinePutResponse with null error list"
    }
  }

  /**
   * This is used to highlight an undefined field.
   */
  val UNDEFINED_FIELD = "Undefined"

  /**
   * Lookup a field in the `otherInfo` section of a [[TimelineEntity]].
   *
   * @param en entity
   * @param name field name
   * @return the value or the string [[UNDEFINED_FIELD]] if not
   * @throws Exception if the field is not found
   */
  def field(en: TimelineEntity, name: String) : Object = {
    fieldOption(en, name).getOrElse(UNDEFINED_FIELD)
  }

  /**
   * Lookup a field in the `otherInfo` section of a [[TimelineEntity]].
   *
   * @param en entity
   * @param name field name
   * @return the value
   * @throws Exception if the field is not found
   */
  def fieldOption(en: TimelineEntity, name: String) : Option[Object] = {
    Option(en.getOtherInfo.get(name))
  }

  /**
   * Lookup a field in the `otherInfo` section of a [[TimelineEntity]]
   * @param en entity
   * @param name field name
   * @return the value converted to a string
   * @throws Exception if the field is not found
   */
  def stringFieldOption(en: TimelineEntity, name: String): Option[String] = {
    val value = en.getOtherInfo.get(name)
    if (value != null ) {
      Some(value.toString)
    } else {
      None
    }
  }

  /**
   * Lookup a numeric field in the `otherInfo` section of a [[TimelineEntity]],
   * fall back to `defval` if the field is absent or cannot be parsed.
   *
   * @param en entity
   * @param name field name
   * @param defval default value; default is 0L
   * @return the value
   */
  def numberField(en: TimelineEntity, name: String, defval: Long = 0L) : Number = {
    try {
      fieldOption(en, name) match {
        case Some(n: Number) => n
        case _ => defval
      }
    } catch {
      case NonFatal(e) => defval
    }
  }

  /**
   * Take a sequence of timeline events and return an ordered list of spark events.
   *
   * Important: this reverses the input in the process.
   * @param events event sequence
   * @return spark event sequence
   */
  def asSparkEvents(events: Seq[TimelineEvent]): Seq[SparkListenerEvent] = {
    events.reverse.map { event =>
      toSparkEvent(event)
    }
  }

  /**
   * Build date for display in status messages.
   *
   * @param timestamp time in milliseconds post-Epoch
   * @param unset string to use if timestamp == 0
   * @return a string for messages
   */
  def humanDateCurrentTZ(timestamp: Long, unset: String) : String = {
    if (timestamp == 0) {
      unset
    } else {
      val dateFormatter = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.LONG)
      dateFormatter.format(timestamp)
    }
  }

  /**
   * Short formatted time.
   *
   * @param timestamp time in milliseconds post-Epoch
   * @param unset string to use if timestamp == 0
   * @return a string for messages
   */
  def timeShort(timestamp: Long, unset: String) : String = {
    if (timestamp == 0) {
      unset
    } else {
      val dateFormatter = DateFormat.getTimeInstance(DateFormat.SHORT)
      dateFormatter.format(timestamp)
    }
  }

  /**
   * Generate the timeline entity ID from the application and attempt ID.
   * This is required to be unique across all entities in the timeline server.
   *
   * @param yarnAppId yarn application ID as passed in during creation
   * @param yarnAttemptId YARN attempt ID as passed in during creation
   */
  def buildEntityId(yarnAppId: ApplicationId,
      yarnAttemptId: Option[ApplicationAttemptId]): String = {
    yarnAttemptId match {
      case Some(aid) => aid.toString
      case None => yarnAppId.toString
    }
  }

  /**
   * Generate the application ID for use in entity fields from the application and attempt ID.
   *
   * @param yarnAppId yarn application ID as passed in during creation
   */
  def buildApplicationIdField(yarnAppId: ApplicationId): String = {
    yarnAppId.toString
  }

  /**
   * Generate an attempt ID for use in the timeline entity "other/app_id" field
   * from the application and attempt ID.
   *
   * This is not guaranteed to be unique across all entities. It is
   * only required to be unique across all attempts of an application.
   *
   * If the application doesn't have an attempt ID, then it is
   * an application instance which, implicitly, is single-attempt.
   * The value [[SINGLE_ATTEMPT]] is returned
   * @param sparkAttemptId attempt ID
   * @return the attempt ID.
   */
  def buildApplicationAttemptIdField(sparkAttemptId: Option[String]): String = {
    sparkAttemptId.getOrElse(SINGLE_ATTEMPT)
  }

  /**
   * Add a filter and field if the value is set.
   *
   * @param entity entity to update
   * @param name filter/field name
   * @param value optional value
   */
  private def addFilterAndField(entity: TimelineEntity,
      name: String, value: Option[String]): Unit = {
    value.foreach { v => addFilterAndField(entity, name, v) }
  }

  /**
   * Add a filter and field.
   *
   * @param entity entity to update
   * @param name filter/field name
   * @param value value
   */
  private def addFilterAndField(entity: TimelineEntity, name: String, value: String): Unit = {
    entity.addPrimaryFilter(name, value)
    entity.addOtherInfo(name, value)
  }

  /**
   * Generate the entity ID from the application and attempt ID.
   * Current policy is to use the attemptId, falling back to the YARN application ID.
   *
   * @param appId yarn application ID as passed in during creation
   * @param attemptId yarn application ID
   * @param sparkApplicationId application ID as submitted in the application start event
   * @param sparkApplicationAttemptId attempt ID, or `None`
   * @param appName application name
   * @param userName user name
   * @param startTime time in milliseconds when this entity was started (must be non zero)
   * @param endTime time in milliseconds when this entity was last updated (0 means not ended)
   * @param lastUpdated time in milliseconds when this entity was last updated (0 leaves unset)
   * @return the timeline entity
   */
  def createTimelineEntity(
      appId: ApplicationId,
      attemptId: Option[ApplicationAttemptId],
      sparkApplicationId: Option[String],
      sparkApplicationAttemptId: Option[String],
      appName: String,
      userName: String,
      startTime: Long, endTime: Long,
      lastUpdated: Long): TimelineEntity = {
    require(appId != null, "no application Id")
    require(appName != null, "no application name")
    require(startTime > 0, "no start time")

    val entity: TimelineEntity = new TimelineEntity()
    val entityId = buildEntityId(appId, attemptId)
    val appIdField = buildApplicationIdField(appId)
    entity.setEntityType(SPARK_EVENT_ENTITY_TYPE)
    entity.setEntityId(entityId)
    // add app/attempt ID information
    addFilterAndField(entity, FIELD_APPLICATION_ID, appIdField)

    entity.addOtherInfo(FIELD_ATTEMPT_ID,
      buildApplicationAttemptIdField(sparkApplicationAttemptId))
    entity.addOtherInfo(FIELD_APP_NAME, appName)
    entity.addOtherInfo(FIELD_APP_USER, userName)
    entity.addOtherInfo(FIELD_SPARK_VERSION, spark.SPARK_VERSION)
    entity.addOtherInfo(FIELD_ENTITY_VERSION, entityVersionCounter.getAndIncrement())
    started(entity, startTime)
    if (endTime != 0) {
      entity.addPrimaryFilter(FILTER_APP_END, FILTER_APP_END_VALUE)
      entity.addOtherInfo(FIELD_END_TIME, endTime)
    }
    if (lastUpdated != 0) {
      entity.addOtherInfo(FIELD_LAST_UPDATED, lastUpdated)
    }
    entity
  }

  /**
   * Add the information to declare that an application has finished and that
   * it has a start time and an end time.
   *
   * @param entity entity to update
   * @param startTime start time
   * @param endtime end time
   * @param sparkApplicationId app ID
   * @param sparkApplicationAttemptId optional attempt ID
   * @return the updated entity
   */
  def completed(
      entity: TimelineEntity,
      startTime: Long,
      endtime: Long,
      sparkApplicationId: Option[String],
      sparkApplicationAttemptId: Option[String]): TimelineEntity = {
    entity.addOtherInfo(FIELD_ATTEMPT_ID,
      buildApplicationAttemptIdField(sparkApplicationAttemptId))
    // set the start info
    started(entity, startTime)
    // add the end info
    entity.addPrimaryFilter(FILTER_APP_END, FILTER_APP_END_VALUE)
    entity.addOtherInfo(FIELD_END_TIME, endtime)
    // this must be the end time
    entity.addOtherInfo(FIELD_LAST_UPDATED, endtime)
    entity
  }

  /**
   * Add the information to declare that an application has started and that
   * it has a start time.
   *
   * @param entity entity to update
   * @param startTime start time.
   * @return the updated entity
   */
  def started(entity: TimelineEntity, startTime: Long): TimelineEntity = {
    entity.addPrimaryFilter(FILTER_APP_START, FILTER_APP_START_VALUE)
    entity.setStartTime(startTime)
    entity.addOtherInfo(FIELD_START_TIME, startTime)
    entity.addOtherInfo(FIELD_LAST_UPDATED, startTime)
    entity
  }

  /**
   * Simple sanity checks for endpoint address, including hostname lookup.
   *
   * This can be used to help validate the address on startup, to postpone
   * later delays.
   *
   * @param endpoint address of service to talk to
   * @return the URL passed in
   */
  def validateEndpoint(endpoint: URI): URI = {
    val host = endpoint.getHost
    if (host == null || host == "0.0.0.0") {
      throw new NoRouteToHostException(s"Invalid host in $endpoint" +
          s" - see https://wiki.apache.org/hadoop/UnsetHostnameOrPort")
    }
    val port = endpoint.getPort
    if (port == 0) {
      throw new NoRouteToHostException(s"Invalid Port in $endpoint" +
          s" - see https://wiki.apache.org/hadoop/UnsetHostnameOrPort")
    }
    // get the address; will trigger a hostname lookup failure if the
    // host is not resolveable.
    val addr = new InetSocketAddress(host, port)
    endpoint
  }
}
