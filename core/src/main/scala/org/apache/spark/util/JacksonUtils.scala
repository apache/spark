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

package org.apache.spark.util

import java.io.{ByteArrayOutputStream, OutputStream, Reader, Writer}
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.core.{JsonEncoding, JsonGenerator}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.ClassTagExtensions
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.JavaTypeable

object JacksonUtils {


  private val mapper = {
    val ret = new ObjectMapper() with ClassTagExtensions
    ret.registerModule(DefaultScalaModule)
    ret
  }

  def writeValuePrettyAsString(o: Any): String =
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o)

  def writeValueAsString(o: Any): String = mapper.writeValueAsString(o)

  def writeValueAsBytes(o: Any): Array[Byte] = mapper.writeValueAsBytes(o)

  def writeValue(out: OutputStream, o: Any): Unit = mapper.writeValue(out, o)

  def writeValue(writer: Writer, o: Any): Unit = mapper.writeValue(writer, o)

  def readValue[T](value: String, valueType: Class[T]): T =
    mapper.readValue(value, valueType)

  def readValue[T: JavaTypeable](content: String): T = mapper.readValue[T](content)

  def readValue[T: JavaTypeable](reader: Reader): T = mapper.readValue[T](reader)

  def readValue[T](reader: Reader, valueType: Class[T]): T =
    mapper.readValue(reader: Reader, valueType: Class[T])

  def readTree(content: String): JsonNode = mapper.readTree(content)

  def toJsonString(block: JsonGenerator => Unit): String = {
    val baos = new ByteArrayOutputStream()
    val generator = mapper.createGenerator(baos, JsonEncoding.UTF8)
    block(generator)
    generator.close()
    baos.close()
    new String(baos.toByteArray, StandardCharsets.UTF_8)
  }

  def toPrettyJsonString(block: JsonGenerator => Unit): String = {
    val baos = new ByteArrayOutputStream()
    val generator = mapper.writerWithDefaultPrettyPrinter()
      .createGenerator(baos, JsonEncoding.UTF8)
    block(generator)
    generator.close()
    baos.close()
    new String(baos.toByteArray, StandardCharsets.UTF_8)
  }
}
