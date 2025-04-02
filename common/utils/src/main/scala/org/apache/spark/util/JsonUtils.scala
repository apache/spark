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

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.core.{JsonEncoding, JsonGenerator}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.util.SparkErrorUtils.tryWithResource

private[spark] trait JsonUtils {

  protected val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJsonString(block: JsonGenerator => Unit): String = {
    tryWithResource(new ByteArrayOutputStream()) { baos =>
      tryWithResource(mapper.createGenerator(baos, JsonEncoding.UTF8)) { generator =>
        block(generator)
      }
      new String(baos.toByteArray, StandardCharsets.UTF_8)
    }
  }
}

private[spark] object JsonUtils extends JsonUtils
