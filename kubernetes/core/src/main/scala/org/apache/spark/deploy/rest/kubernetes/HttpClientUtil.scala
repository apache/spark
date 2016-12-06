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

import javax.net.ssl.{SSLContext, SSLSocketFactory, X509TrustManager}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import feign.Feign
import feign.Request.Options
import feign.jackson.{JacksonDecoder, JacksonEncoder}
import feign.jaxrs.JAXRSContract
import okhttp3.OkHttpClient
import scala.reflect.ClassTag

import org.apache.spark.status.api.v1.JacksonMessageWriter

private[spark] object HttpClientUtil {

  def createClient[T: ClassTag](
      uri: String,
      sslSocketFactory: SSLSocketFactory = SSLContext.getDefault.getSocketFactory,
      trustContext: X509TrustManager = null,
      readTimeoutMillis: Int = 20000,
      connectTimeoutMillis: Int = 20000): T = {
    var httpClientBuilder = new OkHttpClient.Builder()
    Option.apply(trustContext).foreach(context => {
      httpClientBuilder = httpClientBuilder.sslSocketFactory(sslSocketFactory, context)
    })
    val objectMapper = new ObjectMapper()
      .registerModule(new DefaultScalaModule)
      .setDateFormat(JacksonMessageWriter.makeISODateFormat)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    Feign.builder()
      .client(new feign.okhttp.OkHttpClient(httpClientBuilder.build()))
      .contract(new JAXRSContract)
      .encoder(new JacksonEncoder(objectMapper))
      .decoder(new JacksonDecoder(objectMapper))
      .options(new Options(connectTimeoutMillis, readTimeoutMillis))
      .target(clazz, uri)
  }
}
