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

import java.text.SimpleDateFormat
import java.util.{SimpleTimeZone, Calendar}
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
import javax.ws.rs.ext.{ContextResolver, Provider}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{SerializationFeature, ObjectMapper}

@Provider
@Produces(Array(MediaType.APPLICATION_JSON))
class CustomObjectMapper extends ContextResolver[ObjectMapper]{
  val mapper = new ObjectMapper() {
    override def writeValueAsString(t: Any): String = {
      super.writeValueAsString(t)
    }
  }
  mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
  mapper.enable(SerializationFeature.INDENT_OUTPUT)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  mapper.setDateFormat(CustomObjectMapper.makeISODateFormat)

  override def getContext(tpe: Class[_]): ObjectMapper = {
    mapper
  }

}

object CustomObjectMapper {
  def makeISODateFormat: SimpleDateFormat = {
    val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'");
    val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));
    iso8601.setCalendar(cal);
    iso8601;
  }

}
