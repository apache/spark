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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}
import org.apache.spark.util.ArrayImplicits._

/**
 * Attributes that describe a vector ML column.
 *
 * @param name name of the attribute group (the ML column name)
 * @param numAttributes optional number of attributes. At most one of `numAttributes` and `attrs`
 *                      can be defined.
 * @param attrs optional array of attributes. Attribute will be copied with their corresponding
 *              indices in the array.
 */
class AttributeGroup private (
    val name: String,
    val numAttributes: Option[Int],
    attrs: Option[Array[Attribute]]) extends Serializable {

  require(name.nonEmpty, "Cannot have an empty string for name.")
  require(!(numAttributes.isDefined && attrs.isDefined),
    "Cannot have both numAttributes and attrs defined.")

  /**
   * Creates an attribute group without attribute info.
   * @param name name of the attribute group
   */
  def this(name: String) = this(name, None, None)

  /**
   * Creates an attribute group knowing only the number of attributes.
   * @param name name of the attribute group
   * @param numAttributes number of attributes
   */
  def this(name: String, numAttributes: Int) = this(name, Some(numAttributes), None)

  /**
   * Creates an attribute group with attributes.
   * @param name name of the attribute group
   * @param attrs array of attributes. Attributes will be copied with their corresponding indices in
   *              the array.
   */
  def this(name: String, attrs: Array[Attribute]) = this(name, None, Some(attrs))

  /**
   * Optional array of attributes. At most one of `numAttributes` and `attributes` can be defined.
   */
  val attributes: Option[Array[Attribute]] = attrs.map(_.iterator.zipWithIndex
    .map { case (attr, i) => attr.withIndex(i) }.toArray)

  private lazy val nameToIndex: Map[String, Int] = {
    attributes.map(_.iterator.flatMap { attr => attr.name.map(_ -> attr.index.get)}.toMap)
      .getOrElse(Map.empty)
  }

  /** Size of the attribute group. Returns -1 if the size is unknown. */
  def size: Int = {
    if (numAttributes.isDefined) {
      numAttributes.get
    } else if (attributes.isDefined) {
      attributes.get.length
    } else {
      -1
    }
  }

  /** Test whether this attribute group contains a specific attribute. */
  def hasAttr(attrName: String): Boolean = nameToIndex.contains(attrName)

  /** Index of an attribute specified by name. */
  def indexOf(attrName: String): Int = nameToIndex(attrName)

  /** Gets an attribute by its name. */
  def apply(attrName: String): Attribute = {
    attributes.get(indexOf(attrName))
  }

  /** Gets an attribute by its name. */
  def getAttr(attrName: String): Attribute = this(attrName)

  /** Gets an attribute by its index. */
  def apply(attrIndex: Int): Attribute = attributes.get(attrIndex)

  /** Gets an attribute by its index. */
  def getAttr(attrIndex: Int): Attribute = this(attrIndex)

  /** Converts to metadata without name. */
  private[attribute] def toMetadataImpl: Metadata = {
    import AttributeKeys._
    val bldr = new MetadataBuilder()
    if (attributes.isDefined) {
      val numericMetadata = ArrayBuffer.empty[Metadata]
      val nominalMetadata = ArrayBuffer.empty[Metadata]
      val binaryMetadata = ArrayBuffer.empty[Metadata]
      attributes.get.foreach {
        case numeric: NumericAttribute =>
          // Skip default numeric attributes.
          if (numeric.withoutIndex != NumericAttribute.defaultAttr) {
            numericMetadata += numeric.toMetadataImpl(withType = false)
          }
        case nominal: NominalAttribute =>
          nominalMetadata += nominal.toMetadataImpl(withType = false)
        case binary: BinaryAttribute =>
          binaryMetadata += binary.toMetadataImpl(withType = false)
        case UnresolvedAttribute =>
      }
      val attrBldr = new MetadataBuilder
      if (numericMetadata.nonEmpty) {
        attrBldr.putMetadataArray(AttributeType.Numeric.name, numericMetadata.toArray)
      }
      if (nominalMetadata.nonEmpty) {
        attrBldr.putMetadataArray(AttributeType.Nominal.name, nominalMetadata.toArray)
      }
      if (binaryMetadata.nonEmpty) {
        attrBldr.putMetadataArray(AttributeType.Binary.name, binaryMetadata.toArray)
      }
      bldr.putMetadata(ATTRIBUTES, attrBldr.build())
      bldr.putLong(NUM_ATTRIBUTES, attributes.get.length)
    } else if (numAttributes.isDefined) {
      bldr.putLong(NUM_ATTRIBUTES, numAttributes.get)
    }
    bldr.build()
  }

  /** Converts to ML metadata with some existing metadata. */
  def toMetadata(existingMetadata: Metadata): Metadata = {
    new MetadataBuilder()
      .withMetadata(existingMetadata)
      .putMetadata(AttributeKeys.ML_ATTR, toMetadataImpl)
      .build()
  }

  /** Converts to ML metadata */
  def toMetadata(): Metadata = toMetadata(Metadata.empty)

  /** Converts to a StructField with some existing metadata. */
  def toStructField(existingMetadata: Metadata): StructField = {
    StructField(name, new VectorUDT, nullable = false, toMetadata(existingMetadata))
  }

  /** Converts to a StructField. */
  def toStructField(): StructField = toStructField(Metadata.empty)

  override def equals(other: Any): Boolean = {
    other match {
      case o: AttributeGroup =>
        (name == o.name) &&
          (numAttributes == o.numAttributes) &&
          (attributes.map(_.toImmutableArraySeq) == o.attributes.map(_.toImmutableArraySeq))
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + numAttributes.hashCode
    sum = 37 * sum + attributes.map(_.toImmutableArraySeq).hashCode
    sum
  }

  override def toString: String = toMetadata().toString
}

/**
 * Factory methods to create attribute groups.
 */
object AttributeGroup {

  import AttributeKeys._

  /** Creates an attribute group from a [[Metadata]] instance with name. */
  private[attribute] def fromMetadata(metadata: Metadata, name: String): AttributeGroup = {
    import org.apache.spark.ml.attribute.AttributeType._
    if (metadata.contains(ATTRIBUTES)) {
      val numAttrs = metadata.getLong(NUM_ATTRIBUTES).toInt
      val attributes = new Array[Attribute](numAttrs)
      val attrMetadata = metadata.getMetadata(ATTRIBUTES)
      if (attrMetadata.contains(Numeric.name)) {
        attrMetadata.getMetadataArray(Numeric.name)
          .map(NumericAttribute.fromMetadata)
          .foreach { attr =>
          attributes(attr.index.get) = attr
        }
      }
      if (attrMetadata.contains(Nominal.name)) {
        attrMetadata.getMetadataArray(Nominal.name)
          .map(NominalAttribute.fromMetadata)
          .foreach { attr =>
          attributes(attr.index.get) = attr
        }
      }
      if (attrMetadata.contains(Binary.name)) {
        attrMetadata.getMetadataArray(Binary.name)
          .map(BinaryAttribute.fromMetadata)
          .foreach { attr =>
          attributes(attr.index.get) = attr
        }
      }
      var i = 0
      while (i < numAttrs) {
        if (attributes(i) == null) {
          attributes(i) = NumericAttribute.defaultAttr
        }
        i += 1
      }
      new AttributeGroup(name, attributes)
    } else if (metadata.contains(NUM_ATTRIBUTES)) {
      new AttributeGroup(name, metadata.getLong(NUM_ATTRIBUTES).toInt)
    } else {
      new AttributeGroup(name)
    }
  }

  /**
   * Creates an attribute group from a `StructField` instance.
   */
  def fromStructField(field: StructField): AttributeGroup = {
    require(field.dataType == new VectorUDT)
    if (field.metadata.contains(ML_ATTR)) {
      fromMetadata(field.metadata.getMetadata(ML_ATTR), field.name)
    } else {
      new AttributeGroup(field.name)
    }
  }
}
