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

package org.apache.spark.sql.catalyst.json

import java.io.{ByteArrayInputStream, InputStream, InputStreamReader}

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.hadoop.io.Text

import org.apache.spark.unsafe.types.UTF8String

private[sql] object CreateJacksonParser extends Serializable {
  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }

  def utf8String(jsonFactory: JsonFactory, record: UTF8String): JsonParser = {
    val bb = record.getByteBuffer
    assert(bb.hasArray)

    val bain = new ByteArrayInputStream(
      bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())

    jsonFactory.createParser(new InputStreamReader(bain, "UTF-8"))
  }

  def text(jsonFactory: JsonFactory, record: Text): JsonParser = {
    jsonFactory.createParser(record.getBytes, 0, record.getLength)
  }

  def inputStream(jsonFactory: JsonFactory, record: InputStream): JsonParser = {
    jsonFactory.createParser(record)
  }
}
