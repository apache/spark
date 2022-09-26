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

import java.io.Reader
import java.net.URL

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.ClassTagExtensions
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.JavaTypeable

/**
 * `JacksonUtils` is a helper class used to delegate the methods of
 *  Jackson `ObjectMapper` and use a singleton `ObjectMapper`.
 */
object JacksonUtils {

  private val mapper = {
    val ret = new ObjectMapper() with ClassTagExtensions
    ret.registerModule(DefaultScalaModule)
    ret
  }

  def writeValuePrettyAsString(o: Any): String =
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o)

  def writeValueAsString(o: Any): String = mapper.writeValueAsString(o)

  def readValue[T: JavaTypeable](content: String): T = mapper.readValue[T](content)

  def readValue[T: JavaTypeable](reader: Reader): T = mapper.readValue[T](reader)

  def readValue[T: JavaTypeable](url: URL): T = mapper.readValue[T](url)
}
