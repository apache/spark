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

package org.apache.spark.sql.connect.client.arrow

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.complex.StructVector

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{BinaryEncoder, PrimitiveIntEncoder}
import org.apache.spark.sql.errors.CompilationErrors
import org.apache.spark.sql.types.{Geography, Geometry}

abstract class GeospatialArrowSerDe[T](typeName: String) {

  def createDeserializer(
      struct: StructVector,
      vectors: Seq[FieldVector],
      timeZoneId: String): ArrowDeserializers.StructFieldSerializer[T] = {
    assertMetadataPresent(vectors)
    val wkbDecoder = ArrowDeserializers.deserializerFor(
      BinaryEncoder,
      vectors
        .find(_.getName == "wkb")
        .getOrElse(throw CompilationErrors.columnNotFoundError("wkb")),
      timeZoneId)
    val sridDecoder = ArrowDeserializers.deserializerFor(
      PrimitiveIntEncoder,
      vectors
        .find(_.getName == "srid")
        .getOrElse(throw CompilationErrors.columnNotFoundError("srid")),
      timeZoneId)
    new ArrowDeserializers.StructFieldSerializer[T](struct) {
      override def value(i: Int): T = createInstance(wkbDecoder.get(i), sridDecoder.get(i))
    }
  }

  def createSerializer(
      struct: StructVector,
      vectors: Seq[FieldVector]): ArrowSerializer.StructSerializer = {
    assertMetadataPresent(vectors)
    new ArrowSerializer.StructSerializer(
      struct,
      Seq(
        new ArrowSerializer.StructFieldSerializer(
          extractor = (v: Any) => extractSrid(v),
          ArrowSerializer.serializerFor(PrimitiveIntEncoder, struct.getChild("srid"))),
        new ArrowSerializer.StructFieldSerializer(
          extractor = (v: Any) => extractBytes(v),
          ArrowSerializer.serializerFor(BinaryEncoder, struct.getChild("wkb")))))
  }

  private def assertMetadataPresent(vectors: Seq[FieldVector]): Unit = {
    assert(vectors.exists(_.getName == "srid"))
    assert(
      vectors.exists(field =>
        field.getName == "wkb" && field.getField.getMetadata
          .containsKey(typeName) && field.getField.getMetadata.get(typeName) == "true"))
  }

  protected def createInstance(wkb: Any, srid: Any): T
  protected def extractSrid(value: Any): Int
  protected def extractBytes(value: Any): Array[Byte]
}

// Geography-specific implementation
class GeographyArrowSerDe extends GeospatialArrowSerDe[Geography]("geography") {
  override protected def createInstance(wkb: Any, srid: Any): Geography =
    Geography.fromWKB(wkb.asInstanceOf[Array[Byte]], srid.asInstanceOf[Int])

  override protected def extractSrid(value: Any): Int =
    value.asInstanceOf[Geography].getSrid

  override protected def extractBytes(value: Any): Array[Byte] =
    value.asInstanceOf[Geography].getBytes
}

// Geometry-specific implementation
class GeometryArrowSerDe extends GeospatialArrowSerDe[Geometry]("geometry") {
  override protected def createInstance(wkb: Any, srid: Any): Geometry =
    Geometry.fromWKB(wkb.asInstanceOf[Array[Byte]], srid.asInstanceOf[Int])

  override protected def extractSrid(value: Any): Int =
    value.asInstanceOf[Geometry].getSrid

  override protected def extractBytes(value: Any): Array[Byte] =
    value.asInstanceOf[Geometry].getBytes
}
