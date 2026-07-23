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

package org.apache.spark.sql.types

import java.io.OutputStream

import com.fasterxml.jackson.core.{JsonEncoding, JsonGenerator}
import com.fasterxml.jackson.core.io.SegmentedStringWriter
import com.fasterxml.jackson.databind.SerializerProvider
import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, JValueSerializer}

/**
 * Serializes a [[DataType]] to the JSON returned by [[DataType.json]] without first building the
 * type's full `jsonValue` AST. Building the whole AST holds every node live at once, so peak memory
 * scales with the node count of the schema rather than the output size; for a large schema that
 * both spikes the driver heap and churns short-lived garbage. Emitting through one
 * [[JsonGenerator]] holds at most a single field's AST at a time.
 *
 * [[StructField.jsonValue]] does more than emit name/type/nullable/metadata: for a string type
 * with special properties (including one nested in an array or map) it moves those properties off
 * the type and into the field's metadata. A field is emitted by delegating to it whenever that move
 * could happen; only when it cannot (see `canStreamFieldType`) is the field written directly here
 * so its type streams through this object instead of being materialized as an AST. A struct-typed
 * field always qualifies, since the move stops at a struct boundary - a nested struct's own fields
 * carry it - which is what lets a large nested schema stream.
 */
private[sql] object DataTypeJsonStreaming {

  private val mapper = JsonMethods.mapper

  // Reused so each field's value does not re-resolve a serializer.
  private val jvalueSerializer = new JValueSerializer

  private def newProvider(): SerializerProvider = mapper.getSerializerProviderInstance()

  private def writeJValue(prov: SerializerProvider, gen: JsonGenerator, value: JValue): Unit =
    jvalueSerializer.serialize(value, gen, prov)

  def json(dt: DataType): String = {
    val factory = mapper.getFactory
    // SegmentedStringWriter grows by appending recycled buffer segments; a plain StringWriter would
    // reallocate and copy an ever-larger char[] as the output grows.
    val writer = new SegmentedStringWriter(factory._getBufferRecycler())
    val gen = factory.createGenerator(writer)
    try streamDataType(newProvider(), gen, dt)
    finally gen.close() // close, not flush, so the buffer returns to the recycler
    writer.getAndClear()
  }

  /** Streams straight to `out`; the caller owns `out` and this does not close it. */
  def writeJson(dt: DataType, out: OutputStream): Unit = {
    val gen = mapper.getFactory.createGenerator(out, JsonEncoding.UTF8)
    // Let close() recycle the generator's buffer without also closing `out`.
    gen.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
    try streamDataType(newProvider(), gen, dt)
    finally gen.close()
  }

  private def streamDataType(prov: SerializerProvider, gen: JsonGenerator, dt: DataType): Unit =
    dt match {
      case s: StructType =>
        gen.writeStartObject()
        gen.writeStringField("type", s.typeName)
        gen.writeArrayFieldStart("fields")
        s.fields.foreach(field => streamField(prov, gen, field))
        gen.writeEndArray()
        gen.writeEndObject()

      case a: ArrayType =>
        gen.writeStartObject()
        gen.writeStringField("type", a.typeName)
        gen.writeFieldName("elementType")
        streamDataType(prov, gen, a.elementType)
        gen.writeBooleanField("containsNull", a.containsNull)
        gen.writeEndObject()

      case m: MapType =>
        gen.writeStartObject()
        gen.writeStringField("type", m.typeName)
        gen.writeFieldName("keyType")
        streamDataType(prov, gen, m.keyType)
        gen.writeFieldName("valueType")
        streamDataType(prov, gen, m.valueType)
        gen.writeBooleanField("valueContainsNull", m.valueContainsNull)
        gen.writeEndObject()

      case other =>
        writeJValue(prov, gen, other.jsonValue)
    }

  private def streamField(prov: SerializerProvider, gen: JsonGenerator, field: StructField): Unit =
    if (canStreamFieldType(field.dataType)) {
      gen.writeStartObject()
      gen.writeStringField("name", field.name)
      gen.writeFieldName("type")
      streamDataType(prov, gen, field.dataType)
      gen.writeBooleanField("nullable", field.nullable)
      gen.writeFieldName("metadata")
      writeJValue(prov, gen, field.metadata.jsonValue)
      gen.writeEndObject()
    } else {
      writeJValue(prov, gen, field.jsonValue)
    }

  /**
   * Whether a field of type `dt` can be written directly (name/type/nullable/metadata) instead of
   * through [[StructField.jsonValue]]. It can when `StructField.jsonValue` would not move anything
   * into the field's metadata, i.e. when no collated string is reachable without crossing a struct:
   *
   *  - a struct never has anything moved onto the enclosing field (its own fields carry it), so a
   *    struct-typed field is always safe - even one containing collated strings deeper down;
   *  - any other type is safe only if it contains no collated string at all. This over-approximates
   *    "no collation to move" (e.g. array<struct<collated>> is safe but not recognised here) but
   *    never misclassifies a field that does need the move.
   */
  private def canStreamFieldType(dt: DataType): Boolean = dt match {
    case _: StructType => true
    case _ => !dt.existsRecursively {
      case s: StringType => !s.isUTF8BinaryCollation
      case _ => false
    }
  }
}
