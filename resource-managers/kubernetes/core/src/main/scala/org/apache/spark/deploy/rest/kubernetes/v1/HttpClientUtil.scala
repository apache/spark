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
package org.apache.spark.deploy.rest.kubernetes.v1

import java.io.IOException
import java.net.{InetSocketAddress, ProxySelector, SocketAddress, URI}
import java.util.Collections
import javax.net.ssl.{SSLContext, SSLSocketFactory, X509TrustManager}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import feign.{Client, Feign, Request, Response}
import feign.Request.Options
import feign.jackson.{JacksonDecoder, JacksonEncoder}
import feign.jaxrs.JAXRSContract
import io.fabric8.kubernetes.client.Config
import okhttp3.OkHttpClient
import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1.JacksonMessageWriter

private[spark] object HttpClientUtil extends Logging {

  def createClient[T: ClassTag](
      uris: Set[String],
      maxRetriesPerServer: Int = 1,
      sslSocketFactory: SSLSocketFactory = SSLContext.getDefault.getSocketFactory,
      trustContext: X509TrustManager = null,
      readTimeoutMillis: Int = 20000,
      connectTimeoutMillis: Int = 20000): T = {
    var httpClientBuilder = new OkHttpClient.Builder()
    Option.apply(trustContext).foreach(context => {
      httpClientBuilder = httpClientBuilder.sslSocketFactory(sslSocketFactory, context)
    })
    val uriObjects = uris.map(URI.create)
    val httpUris = uriObjects.filter(uri => uri.getScheme == "http")
    val httpsUris = uriObjects.filter(uri => uri.getScheme == "https")
    val maybeAllProxy = Option.apply(System.getProperty(Config.KUBERNETES_ALL_PROXY))
    val maybeHttpProxy = Option.apply(System.getProperty(Config.KUBERNETES_HTTP_PROXY))
      .orElse(maybeAllProxy)
      .map(uriStringToProxy)
    val maybeHttpsProxy = Option.apply(System.getProperty(Config.KUBERNETES_HTTPS_PROXY))
      .orElse(maybeAllProxy)
      .map(uriStringToProxy)
    val maybeNoProxy = Option.apply(System.getProperty(Config.KUBERNETES_NO_PROXY))
      .map(_.split(","))
      .toSeq
      .flatten
    val proxySelector = new ProxySelector {
      override def select(uri: URI): java.util.List[java.net.Proxy] = {
        val directProxy = java.net.Proxy.NO_PROXY
        val resolvedProxy = maybeNoProxy.find( _ == uri.getHost)
          .map( _ => directProxy)
          .orElse(uri.getScheme match {
            case "http" =>
              logDebug(s"Looking up http proxies to route $uri")
              maybeHttpProxy.filter { _ =>
                matchingUriExists(uri, httpUris)
              }
            case "https" =>
              logDebug(s"Looking up https proxies to route $uri")
              maybeHttpsProxy.filter { _ =>
                matchingUriExists(uri, httpsUris)
              }
            case _ => None
        }).getOrElse(directProxy)
        logDebug(s"Routing $uri through ${resolvedProxy.address()} with proxy" +
          s" type ${resolvedProxy.`type`()}")
        Collections.singletonList(resolvedProxy)
      }

      override def connectFailed(uri: URI, sa: SocketAddress, ioe: IOException) = {
        throw new SparkException(s"Failed to connect to proxy through uri $uri," +
          s" socket address: $sa", ioe)
      }
    }
    httpClientBuilder = httpClientBuilder.proxySelector(proxySelector)
    val objectMapper = new ObjectMapper()
      .registerModule(new DefaultScalaModule)
      .setDateFormat(JacksonMessageWriter.makeISODateFormat)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val target = new MultiServerFeignTarget[T](uris.toSeq, maxRetriesPerServer)
    val baseHttpClient = new feign.okhttp.OkHttpClient(httpClientBuilder.build())
    val resetTargetHttpClient = new Client {
      override def execute(request: Request, options: Options): Response = {
        val response = baseHttpClient.execute(request, options)
        if (response.status() / 100 == 2) {
          target.reset()
        }
        response
      }
    }
    Feign.builder()
      .client(resetTargetHttpClient)
      .contract(new JAXRSContract)
      .encoder(new JacksonEncoder(objectMapper))
      .decoder(new JacksonDecoder(objectMapper))
      .options(new Options(connectTimeoutMillis, readTimeoutMillis))
      .retryer(target)
      .target(target)
  }

  private def matchingUriExists(uri: URI, httpUris: Set[URI]): Boolean = {
    httpUris.exists(httpUri => {
      httpUri.getScheme == uri.getScheme && httpUri.getHost == uri.getHost &&
      httpUri.getPort == uri.getPort
    })
  }

  private def uriStringToProxy(uriString: String): java.net.Proxy = {
    val uriObject = URI.create(uriString)
    new java.net.Proxy(java.net.Proxy.Type.HTTP,
      new InetSocketAddress(uriObject.getHost, uriObject.getPort))
  }
}
