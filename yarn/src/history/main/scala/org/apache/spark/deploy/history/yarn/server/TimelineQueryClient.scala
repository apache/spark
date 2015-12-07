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

package org.apache.spark.deploy.history.yarn.server

import java.io.{Closeable, FileNotFoundException}
import java.net.{URI, URL}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import com.sun.jersey.api.client.{Client, ClientResponse, WebResource}
import com.sun.jersey.api.client.config.ClientConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntities, TimelineEntity}
import org.codehaus.jackson.annotate.{JsonAnySetter, JsonIgnoreProperties}
import org.json4s.JString
import org.json4s.jackson.JsonMethods._

import org.apache.spark.Logging
import org.apache.spark.deploy.history.yarn.rest.{HttpRequestException, JerseyBinding, UnauthorizedRequestException}

/**
 * A class to make queries of the Timeline sever through a Jersey client.
 * Exceptions raised by Jersey are extracted and potentially translated.
 * @param timelineURI URI of the timeline service. This is the base URI,
 *                    without the `ws/...` subpath
 * @param conf Hadoop configuration
 * @param jerseyClientConfig Jersey client configuration
 */
private[spark] class TimelineQueryClient(timelineURI: URI,
    conf: Configuration,
    jerseyClientConfig: ClientConfig)
    extends Logging with Closeable {
  require(timelineURI != null, "Null timelineURI")

  /**
   * bool to stop `close()` executing more than once
   */
  private val closed = new AtomicBoolean(false)
  private val timelineURL = timelineURI.toURL
  private val retryLimit = 3
  private val retry_interval = 100
  private val APPLICATION_JSON = "application/json"

  /**
   * the delegation token (unused until delegation support implemented)
   */
  private var token: DelegationTokenAuthenticatedURL.Token =
    new DelegationTokenAuthenticatedURL.Token

  /**
   * The last time there was a token renewal operation.
   */
  private val _lastTokenRenewal = new AtomicLong(0)
  private val _tokenRenewalCount = new AtomicLong(0)

  /**
   * Jersey binding -this exposes the method to reset the token
   */
  private val jerseyBinding = new JerseyBinding(conf, token)

  /**
   * Jersey Client using config from constructor
   */
  private val jerseyClient: Client = jerseyBinding.createClient(conf, jerseyClientConfig)

  /**
   * Base resource of ATS
   */
  private val timelineResource = jerseyClient.resource(timelineURI)

  /**
   * Trigger initialization
   */
  init()

  private def init(): Unit = {
    logDebug("logging in ")
    // this operation has side effects including triggering a refresh thread, if not already running
    val user = UserGroupInformation.getLoginUser()
    logInfo(s"User = $user")
    // now do an initial checkin
    UserGroupInformation.getCurrentUser.checkTGTAndReloginFromKeytab()
  }

  /**
   * When this instance is closed, the jersey client is stopped
   */
  override def close(): Unit = {
    if (!closed.getAndSet(true)) {
      jerseyClient.destroy()
    }
  }

  /**
   * Get the timeline URI
   * @return
   */
  def getTimelineURI() : URI = { timelineURI }

  /**
   * Construct a URI under the timeline service URI
   * @param path subpath
   * @return a new URI
   */
  def uri(path: String): URI = {
    new URL(timelineURL, path).toURI
  }

  /**
   * Get a resource under the service
   * @param path path
   * @return a new resource
   */
  def subresource(path: String): WebResource = {
    timelineResource.path(path)
  }

  /**
   * Execute a GET operation against a specific URI, uprating jersey faults
   * into more specific exceptions
   * @param uri URI (used when handling exceptions)
   * @param action action to perform
   * @tparam T type of response
   * @return the result of the action
   */
  def get[T](uri: URI, action: (() => T)): T = {
    exec("GET", uri, action)
  }

  /**
   * Execute an HTTP operation against a specific URI, uprating jersey faults
   * into more specific exceptions.
   * Some operations may fail-fast, if they are considered to be non-recoverable
   * @param uri URI (used when generating text reporting exceptions)
   * @param action action to perform
   * @param retries  number of retries on any failed operation
   * @tparam T type of response
   * @return the result of the action
   */
  def exec[T](verb: String, uri: URI, action: (() => T), retries: Int = retryLimit): T = {
    logDebug(s"$verb $uri")
    try {
      innerExecAction(action)
    } catch {
      case e: Exception =>
        val exception = JerseyBinding.translateException(verb, uri, e)
        logDebug(s"$verb $uri failed: $exception", exception)
        exception match {
          case notFound: FileNotFoundException =>
            logInfo(s"Not found: $uri")
            // it's not likely to come back: fail fast
            throw notFound

          case ure: UnauthorizedRequestException =>
            // possible expiry
            logInfo(s"Renewing Auth token due to $exception")
            resetConnection()

          case other: Exception =>
            logWarning(s"$verb $uri failed: $exception")
            logDebug(s"detail", exception)
        }
        if (retries > 0) {
          logInfo(s"Retrying -remaining attempts: $retries")
          Thread.sleep(retry_interval)
          exec(verb, uri, action, retries - 1)
        } else {
          throw exception
        }
    }
  }

  /**
   * Reset the delegation token. Also triggers a TGT login,
   * just for completeness
   */
  def resetConnection(): Unit = {
    logInfo("Resetting connection")
    UserGroupInformation.getCurrentUser.checkTGTAndReloginFromKeytab()
    jerseyBinding.resetToken()
    _lastTokenRenewal.set(System.currentTimeMillis())
    _tokenRenewalCount.incrementAndGet()
  }

  /**
   * Invoke the action without any failure handling.
   *
   * This is intended as a point for subclasses to simulate failures
   * and so verify the failure handling code paths.
   * @param action action to perform
   * @tparam T type of response
   * @return the result of the action
   */
  protected def innerExecAction[T](action: () => T): T = {
    action()
  }

  /**
   * Peform an "about" query.
   * @return information about the service.
   */
  def about(): String = {
    val aboutURI = uri("")
    val resource = jerseyClient.resource(aboutURI)
    val body = get(aboutURI, () => resource.accept(APPLICATION_JSON).get(classOf[String]))
    val json = parse(body)
    json \ "About" match {
      case s: JString =>
        s.toString
      case _ =>
        throw new HttpRequestException(200, "GET", aboutURI.toString, body)
    }
  }

  /**
   * This is a low-cost, non-side-effecting timeline service
   * endpoint check operation.
   *
   * It does a GET of the about URL, and verifies
   * it for validity (response type, body).
   * @throws Exception on a failure
   */
  def endpointCheck(): Unit = {
    val aboutURI = uri("")
    val resource = jerseyClient.resource(aboutURI)

    val clientResponse = get(aboutURI,
         () => {
           val response = resource.get(classOf[ClientResponse])
           val status = response.getClientResponseStatus
           if (status.getStatusCode != HttpServletResponse.SC_OK) {
             // error code. Repeat looking for a string and so
             // trigger a failure and the exception conversion logic
             resource.get(classOf[String])
           }
           response
         })

    val endpoint = aboutURI.toString
    val status = clientResponse.getClientResponseStatus.getStatusCode
    val body = clientResponse.getEntity(classOf[String])

    // validate the content type is JSON; if not its usually the wrong URL
    val contentType = clientResponse.getType
    if (MediaType.APPLICATION_JSON_TYPE != contentType) {
      throw new HttpRequestException(status, "GET", endpoint ,
        s"Wrong content type: expected application/json but got $contentType. " +
            TimelineQueryClient.MESSAGE_CHECK_URL + s": $aboutURI",
        body)
    }
    // an empty body is a sign of other problems
    if (body.isEmpty) {
      throw new HttpRequestException(status, "GET", endpoint,
            TimelineQueryClient.MESSAGE_EMPTY_RESPONSE + s": $aboutURI")
    }
    // finally, issue an about() operation again to force the JSON parse
    about()
  }

  /**
   * Add a new query param if the option contains a value; the stringified value of the optional
   * is used as the query parameter value
   * @param resource resource to extend
   * @param name parameter name
   * @param opt option
   * @return a new resource
   */
  private def applyOptionalParam(resource: WebResource,
      name: String,
      opt: Option[Any]): WebResource = {
    opt match {
      case Some(value) => resource.queryParam(name, value.toString)
      case None => resource
    }
  }


  /**
   * Get entities matching the entity type and any optional filters.
   * All parameters other than <code>entityType</code> have
   * default values; None for <code>optional</code>, empty
   * collections for the others.
   * @param entityType entity type
   * @param primaryFilter primary filter
   * @param secondaryFilters map of secondary filters
   * @param fields list of fields to retrieve
   * @param limit limit on how many to retrieve
   * @param windowStart time window to start retrieval
   * @param windowEnd time window to stop retrieval
   * @param fromId optional ID to start from
   * @param fromTs optional timestamp to start from
   * @return a possibly empty list of entities
   */
    def listEntities(entityType: String,
        primaryFilter: Option[(String, String)] = None,
        secondaryFilters: Map[String, String] = Map(),
        fields: Seq[String] = Nil,
        limit: Option[Long] = None,
        windowStart: Option[Long] = None,
        windowEnd: Option[Long] = None,
        fromId: Option[String] = None,
        fromTs: Option[Long] = None): List[TimelineEntity] = {
      require(!entityType.isEmpty, "no entity type")
      var resource = entityResource(entityType)
      // build the resource
      // every application returns a new result, which complicates applying map and list arguments
      // to it. hence the use of a variable
      resource = primaryFilter match {
        case Some((key, value)) =>
          resource.queryParam("primaryFilter", s"$key:$value")
        case None =>
          resource
      }
      secondaryFilters foreach
          ((t: (String, String)) =>
            resource = resource.queryParam("secondaryFilter", s"${t._1}:${t._2}"))
      resource = applyOptionalParam(resource, "windowStart", windowStart)
      resource = applyOptionalParam(resource, "windowEnd", windowEnd)
      resource = applyOptionalParam(resource, "limit", limit)
      resource = applyOptionalParam(resource, "fromId", fromId)
      resource = applyOptionalParam(resource, "fromTs", fromTs)
      if (fields.nonEmpty) {
        resource = resource.queryParam("fields", fields.mkString(","))
      }
      // execute the request
      val response = get(resource.getURI,
          () => resource.accept(APPLICATION_JSON).get(classOf[TimelineEntities]))
      response.getEntities.asScala.toList
  }

  /**
   * Get the resource of an entity type
   * @param entityType entity type
   * @return resource
   */
  def entityResource(entityType: String): WebResource = {
    subresource(entityType)
  }

  /**
   * Get an entity
   * @param entityType type
   * @param entityId the entity
   * @return the entity if it was found
   */
  def getEntity(entityType: String, entityId: String): TimelineEntity = {
    require(!entityId.isEmpty, "no entity ID")
    val resource = entityResource(entityType).path(entityId)
    get(resource.getURI,
        () => resource.accept(APPLICATION_JSON).get(classOf[TimelineEntity]))
  }

  /**
   * toString method returns the URI of the timeline service
   * @return
   */
  override def toString: String = {
    s"Timeline Query Client against $timelineURI"
  }

  /**
   * Get the time the token was last renewed
   * @return a system timestamp of the last renewal; 0 on startup
   */
  def lastTokenRenewal: Long = {
    _lastTokenRenewal.get()
  }

  /**
   * Get count of token renewals
   * @return
   */
  def tokenRenewalCount: Long = {
    _tokenRenewalCount.get()
  }
}

/**
 * Simple About response. The timeline V1 API keeps this type hidden in the server code, even though
 * it is tagged `@Public`
 */
@JsonIgnoreProperties(ignoreUnknown = true)
private[spark] class AboutResponse {

  var other: Map[String, Object] = Map()

  var About: String = _

  @JsonAnySetter
  def handleUnknown(key: String, value: Object): Unit = {
    other += (key -> value)
  }

}

/**
 * Constants associated with the Query API
 */
private[spark] object TimelineQueryClient {
  val MESSAGE_CHECK_URL = "Check the URL of the timeline service:"
  val MESSAGE_EMPTY_RESPONSE = s"No data in the response"
  val EVENTS = "EVENTS"
  val LAST_EVENT_ONLY = "LASTEVENTONLY"
  val OTHER_INFO = "OTHERINFO"
  val PRIMARY_FILTERS = "PRIMARYFILTERS"
  val RELATED_ENTITIES = "RELATEDENTITIES"
}
