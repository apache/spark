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
package org.apache.spark.status.api.v1

import java.io.OutputStream
import java.lang.annotation.Annotation
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, SimpleTimeZone}
import javax.ws.rs.Produces
import javax.ws.rs.core.{MediaType, MultivaluedMap}
import javax.ws.rs.ext.{MessageBodyWriter, Provider}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}

/**
 * This class converts the POJO metric responses into json, using jackson.
 *
 * This doesn't follow the standard jersey-jackson plugin options, because we want to stick
 * with an old version of jersey (since we have it from yarn anyway) and don't want to pull in lots
 * of dependencies from a new plugin.
 *
 * Note that jersey automatically discovers this class based on its package and its annotations.
 */
@Provider
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class JacksonMessageWriter extends MessageBodyWriter[Object]{

  val mapper = new ObjectMapper() {
    override def writeValueAsString(t: Any): String = {
      super.writeValueAsString(t)
    }
  }
  mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
  mapper.setDateFormat(JacksonMessageWriter.makeISODateFormat)

  override def isWriteable(
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType): Boolean = {
      true
  }

  override def writeTo(
      t: Object,
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType,
      multivaluedMap: MultivaluedMap[String, AnyRef],
      outputStream: OutputStream): Unit = {
    mapper.writeValue(outputStream, t)
  }

  override def getSize(
      t: Object,
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType): Long = {
    -1L
  }
}

private[spark] object JacksonMessageWriter {
  def makeISODateFormat: SimpleDateFormat = {
    val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'", Locale.US)
    val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
    iso8601.setCalendar(cal)
    iso8601
  }
}
