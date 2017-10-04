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

package org.apache.spark.ml.attribute

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.{DoubleType, Metadata, MetadataBuilder, StructField}

/**
 * :: DeveloperApi ::
 * The APIs for converting ML Attributes to SQL `Metadata` and `StructField`.
 */
@DeveloperApi
trait MetadataInterface {

  val name: Option[String]

  /**
   * Converts this attribute to `Metadata`.
   */
  def toMetadataImpl(): Metadata

  /** Converts to ML metadata with some existing metadata. */
  def toMetadata(existingMetadata: Metadata): Metadata = {
    new MetadataBuilder()
      .withMetadata(existingMetadata)
      .putMetadata(AttributeKeys.ML_ATTRV2, toMetadataImpl())
      .build()
  }

  /** Converts to ML metadata */
  def toMetadata(): Metadata = toMetadata(Metadata.empty)


  /**
   * Converts to a `StructField` with some existing metadata.
   * @param existingMetadata existing metadata to carry over
   */
  def toStructField(existingMetadata: Metadata): StructField = {
    require(name.isDefined, "Must define attribute name before converting to `StructField`.")
    val newMetadata = toMetadata(existingMetadata)
    StructField(name.get, DoubleType, nullable = false, newMetadata)
  }

  /**
   * Converts to a `StructField`.
   */

  def toStructField(): StructField = toStructField(Metadata.empty)
}

@DeveloperApi
object MLAttributes {
  /**
   * Creates an `Attribute` from a `Metadata` instance.
   */
  def fromMetadata(metadata: Metadata): BaseAttribute = {
    if (metadata.contains(AttributeKeys.TYPE)) {
      val attrType = metadata.getString(AttributeKeys.TYPE)
      getFactory(attrType).fromMetadata(metadata)
    } else {
      UnresolvedMLAttribute
    }
  }

  /** Gets the attribute factory given the attribute type name. */
  private def getFactory(attrType: String): MLAttributeFactory = {
    if (attrType == AttributeType.Numeric.name) {
      NumericAttr
    } else if (attrType == AttributeType.Nominal.name) {
      NominalAttr
    } else if (attrType == AttributeType.Binary.name) {
      BinaryAttr
    } else if (attrType == AttributeType.Vector.name) {
      VectorAttr
    } else {
      throw new IllegalArgumentException(s"Cannot recognize type $attrType.")
    }
  }

  /**
   * Creates an `Attribute` from a `StructField` instance, optionally preserving name.
   */
  private[ml] def fromStructField(field: StructField, preserveName: Boolean): BaseAttribute = {
    val metadata = field.metadata
    val mlAttr = AttributeKeys.ML_ATTRV2
    if (metadata.contains(mlAttr)) {
      val attr = fromMetadata(metadata.getMetadata(mlAttr))
      if (preserveName) {
        attr
      } else {
        attr.withName(field.name)
      }
    } else {
      UnresolvedMLAttribute
    }
  }
}

/** Trait for ML attribute factories. */
@DeveloperApi
trait MLAttributeFactory {
  def fromMetadata(metadata: Metadata): BaseAttribute

  def loadCommonMetadata(metadata: Metadata): (Option[String], Seq[Int]) = {
    val name = if (metadata.contains(AttributeKeys.NAME)) {
      Some(metadata.getString(AttributeKeys.NAME))
    } else {
      None
    }

    val indicesRange = if (metadata.contains(AttributeKeys.INDICES)) {
      metadata.getLongArray(AttributeKeys.INDICES).map(_.toInt).toSeq
    } else {
      Seq.empty
    }

    (name, indicesRange)
  }
}
