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
package org.apache.spark.deploy.rest.kubernetes

import java.io.FileInputStream
import java.net.{InetSocketAddress, URI}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{SSLContext, TrustManagerFactory, X509TrustManager}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.fabric8.kubernetes.client.Config
import okhttp3.{Dispatcher, OkHttpClient}
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory
import retrofit2.converter.scalars.ScalarsConverterFactory

import org.apache.spark.SSLOptions
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] trait RetrofitClientFactory {
  def createRetrofitClient[T](baseUrl: String, serviceType: Class[T], sslOptions: SSLOptions): T
}

private[spark] object RetrofitClientFactoryImpl extends RetrofitClientFactory with Logging {

  private val OBJECT_MAPPER = new ObjectMapper().registerModule(new DefaultScalaModule)
  private val SECURE_RANDOM = new SecureRandom()

  def createRetrofitClient[T](baseUrl: String, serviceType: Class[T], sslOptions: SSLOptions): T = {
    val dispatcher = new Dispatcher(ThreadUtils.newDaemonCachedThreadPool(s"http-client-$baseUrl"))
    val serviceUri = URI.create(baseUrl)
    val maybeAllProxy = Option.apply(System.getProperty(Config.KUBERNETES_ALL_PROXY))
    val serviceUriScheme = serviceUri.getScheme
    val maybeHttpProxy = (if (serviceUriScheme.equalsIgnoreCase("https")) {
      Option.apply(System.getProperty(Config.KUBERNETES_HTTPS_PROXY))
    } else if (serviceUriScheme.equalsIgnoreCase("http")) {
      Option.apply(System.getProperty(Config.KUBERNETES_HTTP_PROXY))
    } else {
      maybeAllProxy
    }).map(uriStringToProxy)
    val maybeNoProxy = Option.apply(System.getProperty(Config.KUBERNETES_NO_PROXY))
      .map(_.split(","))
      .toSeq
      .flatten
    val resolvedProxy = maybeNoProxy.find(_ == serviceUri.getHost)
      .map( _ => java.net.Proxy.NO_PROXY)
      .orElse(maybeHttpProxy)
      .getOrElse(java.net.Proxy.NO_PROXY)
    val okHttpClientBuilder = new OkHttpClient.Builder()
      .dispatcher(dispatcher)
      .proxy(resolvedProxy)
    logDebug(s"Proxying to $baseUrl through address ${resolvedProxy.address()} with proxy of" +
      s" type ${resolvedProxy.`type`()}")
    sslOptions.trustStore.foreach { trustStoreFile =>
      require(trustStoreFile.isFile, s"TrustStore provided at ${trustStoreFile.getAbsolutePath}"
        + " does not exist, or is not a file.")
      val trustStoreType = sslOptions.trustStoreType.getOrElse(KeyStore.getDefaultType)
      val trustStore = KeyStore.getInstance(trustStoreType)
      val trustStorePassword = sslOptions.trustStorePassword.map(_.toCharArray).orNull
      Utils.tryWithResource(new FileInputStream(trustStoreFile)) {
        trustStore.load(_, trustStorePassword)
      }
      val trustManagerFactory = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm)
      trustManagerFactory.init(trustStore)
      val trustManagers = trustManagerFactory.getTrustManagers
      val sslContext = SSLContext.getInstance("TLSv1.2")
      sslContext.init(null, trustManagers, SECURE_RANDOM)
      okHttpClientBuilder.sslSocketFactory(sslContext.getSocketFactory,
        trustManagers(0).asInstanceOf[X509TrustManager])
    }
    new Retrofit.Builder()
      .baseUrl(baseUrl)
      .addConverterFactory(ScalarsConverterFactory.create())
      .addConverterFactory(JacksonConverterFactory.create(OBJECT_MAPPER))
      .client(okHttpClientBuilder.build())
      .build()
      .create(serviceType)
  }

  private def uriStringToProxy(uriString: String): java.net.Proxy = {
    val uriObject = URI.create(uriString)
    new java.net.Proxy(java.net.Proxy.Type.HTTP,
        new InetSocketAddress(uriObject.getHost, uriObject.getPort))
  }
}
