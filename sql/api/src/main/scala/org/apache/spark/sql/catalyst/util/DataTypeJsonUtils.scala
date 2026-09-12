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

package org.apache.spark.sql.catalyst.util

import java.io.StringWriter

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, SerializerProvider}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JValueDeserializer

import org.apache.spark.sql.types.DataType

object DataTypeJsonUtils {
  private val jsonFactory = new JsonFactory()

  def toJson(dataType: DataType): String = {
    val writer = new StringWriter
    val generator = jsonFactory.createGenerator(writer)
    try {
      writeDataType(dataType, generator)
    } finally {
      generator.close()
    }
    writer.toString
  }

  private[sql] def writeDataType(dataType: DataType, generator: JsonGenerator): Unit = {
    dataType.writeJsonTo(generator)
  }

  /**
   * Jackson serializer for [[DataType]].
   */
  class DataTypeJsonSerializer extends JsonSerializer[DataType] {
    override def serialize(
        value: DataType,
        gen: JsonGenerator,
        provider: SerializerProvider): Unit = {
      writeDataType(value, gen)
    }
  }

  /**
   * Jackson deserializer for [[DataType]]. Internally this delegates to json4s based
   * deserialization.
   */
  class DataTypeJsonDeserializer extends JsonDeserializer[DataType] {
    private val delegate = new JValueDeserializer(classOf[Any])

    override def deserialize(
        jsonParser: JsonParser,
        deserializationContext: DeserializationContext): DataType = {
      val json = delegate.deserialize(jsonParser, deserializationContext)
      DataType.parseDataType(json.asInstanceOf[JValue])
    }
  }
}
