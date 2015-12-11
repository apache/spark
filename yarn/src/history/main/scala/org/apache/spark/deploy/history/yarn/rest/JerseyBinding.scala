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
package org.apache.spark.deploy.history.yarn.rest

import java.io.{FileNotFoundException, IOException}
import java.lang.reflect.UndeclaredThrowableException
import java.net.{HttpURLConnection, URI, URL}
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.MediaType

import com.sun.jersey.api.client.{Client, ClientHandlerException, ClientResponse, UniformInterfaceException}
import com.sun.jersey.api.client.config.{ClientConfig, DefaultClientConfig}
import com.sun.jersey.api.json.JSONConfiguration
import com.sun.jersey.client.urlconnection.{HttpURLConnectionFactory, URLConnectionClientHandler}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * Jersey specific integration with the SPNEGO Auth
 * @param conf configuration to build off
 */
private[spark] class JerseyBinding(conf: Configuration,
    token: DelegationTokenAuthenticatedURL.Token)
    extends Logging with HttpURLConnectionFactory {
  private val connector = SpnegoUrlConnector.newInstance(conf, token)
  private val handler = new URLConnectionClientHandler(this)

  override def getHttpURLConnection(url: URL): HttpURLConnection = {
    connector.getHttpURLConnection(url)
  }

  /**
  * reset the token
   */
  def resetToken(): Unit = {
    connector.resetAuthTokens()
  }

  /**
   * Create a Jersey client with the UGI binding set up.
   * @param conf Hadoop configuration
   * @param clientConfig jersey client config
   * @return a new client instance
   */
  def createClient(conf: Configuration, clientConfig: ClientConfig): Client = {
    new Client(handler, clientConfig)
  }
}

private[spark] object JerseyBinding extends Logging {

  /**
   * Translate exceptions, where possible. If not, it is passed through unchanged
   * @param verb HTTP verb
   * @param targetURL URL of operation
   * @param thrown exception caught
   * @return an exception to log, ingore, throw...
   */
  def translateException(
    verb: String,
    targetURL: URI,
    thrown: Throwable): Throwable = {
    thrown match {
      case ex: ClientHandlerException =>
        // client-side Jersey exception
        translateException(verb, targetURL, ex)

      case ex: UniformInterfaceException =>
        // remote Jersey exception
        translateException(verb, targetURL, ex)

      case ex: UndeclaredThrowableException =>
        // wrapped exception raised in a doAs() call. Extract cause and retry
        translateException(verb, targetURL, ex.getCause)

      case _ =>
        // anything else
        thrown
    }
  }

  /**
   * Handle a client-side Jersey exception by extracting the inner cause.
   *
   * If there's an inner IOException, return that.
   *
   * Otherwise: create a new wrapper IOE including verb and target details
   * @param verb HTTP Verb used
   * @param targetURL URL being targeted
   * @param exception original exception
   * @return an exception to throw
   */
  def translateException(
    verb: String,
    targetURL: URI,
    exception: ClientHandlerException): IOException = {
    val uri = if (targetURL !=null) targetURL.toString else "unknown URL"
    exception.getCause match {
      case ioe: IOException =>
        // pass through
        ioe
      case other: Throwable =>
        // get inner cause into exception text
        val ioe = new IOException(s"$verb $uri failed: $exception - $other")
        ioe.initCause(exception)
        ioe
      case _ =>
        // no inner cause
        val ioe = new IOException(s"$verb $uri failed: $exception")
        ioe.initCause(exception)
        ioe
    }
  }

  /**
   * Get the body of a response. A failure to extract the body is logged
   * @param response response
   * @param limit the limit: 0 means "the entire HTTP response"
   * @return string body; may mean "" an empty body or the operation failed.
   */
  def bodyOfResponse(response: ClientResponse, limit: Int = 0) : String = {
    var body: String = ""
    Utils.tryLogNonFatalError {
      if (response.hasEntity) {
        body = response.getEntity(classOf[String])
      }
    }
    // shorten the body
    if (limit > 0) {
      body.substring(0, Math.min(limit, body.length))
    } else {
      body
    }
  }

  /**
   * Convert Jersey exceptions into useful IOExceptions. This includes
   * building an error message which include the URL, verb and status code,
   * logging any text body, and wrapping in an IOException or subclass
   * with that message and the response's exception as a nested exception.
   * @param verb HTTP Verb used
   * @param targetURL URL being targeted
   * @param exception original exception
   * @return a new exception, the original one nested as a cause
   */
  def translateException(
    verb: String,
    targetURL: URI,
    exception: UniformInterfaceException): IOException = {
    var ioe: IOException = null
    val response = exception.getResponse
    val url = if (targetURL != null) targetURL.toString else "unknown URL"
    if (response != null) {
      val status = response.getStatus
      val body = bodyOfResponse(response, 256)
      val errorText = s"Bad $verb request: status code $status against $url; $body"
      status match {

        case HttpServletResponse.SC_UNAUTHORIZED =>
          ioe = new UnauthorizedRequestException(url,
            s"Unauthorized (401) access to $url",
            exception)

        case HttpServletResponse.SC_FORBIDDEN =>
          ioe = new UnauthorizedRequestException(url,
            s"Forbidden (403) access to $url",
            exception)

        case HttpServletResponse.SC_BAD_REQUEST
          | HttpServletResponse.SC_NOT_ACCEPTABLE
          | HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE =>
          throw new IOException(errorText, exception)

        case HttpServletResponse.SC_NOT_FOUND =>
          ioe = new FileNotFoundException(s"$url; $body")
          ioe.initCause(exception)

        case resultCode: Int if resultCode >= 400 && resultCode < 500 =>
          ioe = new FileNotFoundException(
            s"Bad $verb request: status code $status against $url; $body")
          ioe.initCause(exception)
        case _ =>
          ioe = new IOException(errorText, exception)

      }
    } else {
      // no response
      ioe = new IOException(s"$verb $url failed: $exception", exception)
    }
    ioe
  }

  /**
   * Create a Jersey client with the UGI binding set up.
   * @param conf Hadoop configuration
   * @param clientConfig jersey client config
   * @param token optional delegation token
   * @return a new client instance
   */
  def createJerseyClient(
    conf: Configuration,
    clientConfig: ClientConfig,
    token: DelegationTokenAuthenticatedURL.Token = new DelegationTokenAuthenticatedURL.Token)
    : Client = {
    new JerseyBinding(conf, token).createClient(conf, clientConfig)
  }

  /**
   * Create the client config for Jersey.
   * @return the client configuration
   */
  def createClientConfig(): ClientConfig = {
    val cc = new DefaultClientConfig()
    cc.getClasses().add(classOf[JsonJaxbBinding])
    cc.getFeatures.put(JSONConfiguration.FEATURE_POJO_MAPPING, true)
    cc
  }
}

/**
 * Define the jaxb binding for the Jersey client
 */
private[spark] class JsonJaxbBinding extends JacksonJaxbJsonProvider {

  override def locateMapper(classtype: Class[_], mediaType: MediaType): ObjectMapper = {
    val mapper = super.locateMapper(classtype, mediaType)
    val introspector = new JaxbAnnotationIntrospector
    mapper.setAnnotationIntrospector(introspector)
    mapper.setSerializationInclusion(Inclusion.NON_NULL)
    mapper
  }

}
