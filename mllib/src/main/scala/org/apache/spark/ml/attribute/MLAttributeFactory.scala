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
import org.apache.spark.ml.linalg.VectorUDT
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
    val mlAttrV2 = AttributeKeys.ML_ATTRV2
    val mlAttr = AttributeKeys.ML_ATTR

    val attr = if (metadata.contains(mlAttrV2)) {
      fromMetadata(metadata.getMetadata(mlAttrV2))
    } else if (metadata.contains(mlAttr)) {
      // For back-compatibility, to read metadata from previous attribute APIs.
      convertFromV1API(field, metadata.getMetadata(mlAttr))
    } else {
      UnresolvedMLAttribute
    }

    if (preserveName) {
      attr
    } else {
      attr.withName(field.name)
    }
  }

  /**
   * Converts an `Attribute`/`AttributeGroup` to v2 attribute APIs.
   */
  private def convertFromV1API(field: StructField, metadata: Metadata): BaseAttribute = {
    val isVectorField = field.dataType == new VectorUDT

    if (isVectorField) {
      val attrGroup = AttributeGroup.fromMetadata(metadata, field.name)
      convertFromV1AttributeGroup(attrGroup)
    } else {
      val attr = Attribute.fromMetadata(metadata)
      convertFromV1Attributes(attr)
    }
  }

  /**
   * Converts `AttributeGroup` to `VectorAttr`.
   */
  private def convertFromV1AttributeGroup(attrGroup: AttributeGroup): BaseAttribute = {
    val numOfAttributes = attrGroup.size
    if (numOfAttributes != -1) {
      val vecAttr = VectorAttr(numOfAttributes = numOfAttributes)
        .withName(attrGroup.name)
      attrGroup.attributes.map { attrs =>
        attrs.map(convertFromV1Attributes).map(vecAttr.addAttribute)
      }
      vecAttr
    } else {
      UnresolvedMLAttribute
    }
  }

  /**
   * Given an attribute from previous attribute APIs, this method converts it to v2 attribute APIs.
   */
  private def convertFromV1Attributes(attr: Attribute): SimpleAttribute = {
    attr match {
      case nominal: NominalAttribute =>
        convertFromV1NominalAttribute(nominal)
      case binary: BinaryAttribute =>
        convertFromV1BinaryAttribute(binary)
      case numeric: NumericAttribute =>
        convertFromV1NumericAttribute(numeric)
      case _ => throw new IllegalArgumentException(s"Can't convert from unknown attribute $attr")
    }
  }

  private def convertFromV1NominalAttribute(nominal: NominalAttribute): NominalAttr = {
    val name = nominal.name
    val index = nominal.index
    val isOrdinal = nominal.isOrdinal
    val values = if (nominal.values.isDefined) {
      nominal.values.get
    } else if (nominal.numValues.isDefined) {
      (0 until nominal.numValues.get).map(_.toDouble.toString).toArray
    } else {
      null
    }
    NominalAttr(
      name = name,
      indicesRange = index.map(Seq(_)).getOrElse(Seq.empty),
      isOrdinal = isOrdinal,
      values = Option(values))
  }

  private def convertFromV1BinaryAttribute(binary: BinaryAttribute): BinaryAttr = {
    val name = binary.name
    val index = binary.index
    val values = binary.values
    BinaryAttr(
      name = name,
      indicesRange = index.map(Seq(_)).getOrElse(Seq.empty),
      values = values)
  }

  private def convertFromV1NumericAttribute(numeric: NumericAttribute): NumericAttr = {
    val name = numeric.name
    val index = numeric.index
    val min = numeric.min
    val max = numeric.max
    val std = numeric.std
    val sparsity = numeric.sparsity
    NumericAttr(
      name = name,
      indicesRange = index.map(Seq(_)).getOrElse(Seq.empty),
      min = min,
      max = max,
      std = std,
      sparsity = sparsity)
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
      Seq.empty[Int]
    }

    (name, indicesRange)
  }
}
