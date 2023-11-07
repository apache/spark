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

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, SerializerProvider}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.{JValueDeserializer, JValueSerializer}

import org.apache.spark.sql.types.DataType

object DataTypeJsonUtils {
  /**
   * Jackson serializer for [[DataType]]. Internally this delegates to json4s based serialization.
   */
  class DataTypeJsonSerializer extends JsonSerializer[DataType] {
    private val delegate = new JValueSerializer
    override def serialize(
      value: DataType,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
      delegate.serialize(value.jsonValue, gen, provider)
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
