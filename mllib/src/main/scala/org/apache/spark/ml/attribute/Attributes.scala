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
import org.apache.spark.sql.types.{DoubleType, Metadata, MetadataBuilder, NumericType, StructField}

/**
 * :: DeveloperApi ::
 * A nominal attribute.
 * @param isOrdinal whether this attribute is ordinal (optional).
 * @param values optional values.
 */
@DeveloperApi
case class NominalAttr(
    name: Option[String] = None,
    indicesRange: Seq[Int] = Seq.empty,
    isOrdinal: Option[Boolean] = None,
    values: Option[Array[String]] = None) extends SimpleAttribute {

  override val attrType: AttributeType = AttributeType.Numeric

  override def withName(name: String): SimpleAttribute = copy(name = Some(name))
  override def withoutName: SimpleAttribute = copy(name = None)

  override def withIndicesRange(begin: Int, end: Int): SimpleAttribute =
    copy(indicesRange = Seq(begin, end))
  override def withIndicesRange(index: Int): SimpleAttribute = copy(indicesRange = Seq(index))

  override def withoutIndicesRange: SimpleAttribute = copy(indicesRange = Seq.empty)

  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))
    bldr.putLongArray(AttributeKeys.INDICES, indicesRange.toArray.map(_.toLong))
    isOrdinal.foreach(bldr.putBoolean(AttributeKeys.ORDINAL, _))
    values.foreach(v => bldr.putStringArray(AttributeKeys.VALUES, v.toArray))

    bldr.build()
  }

}

/**
 * :: DeveloperApi ::
 * A binary attribute.
 * @param values optional values. If set, its size must be 2.
 */
@DeveloperApi
case class BinaryAttr(
    name: Option[String] = None,
    indicesRange: Seq[Int] = Seq.empty,
    values: Option[Array[String]] = None) extends SimpleAttribute {

  values.foreach { v =>
    require(v.length == 2, s"Number of values must be 2 for a binary attribute but got ${v.toSeq}.")
  }

  override val attrType: AttributeType = AttributeType.Binary

  override def withName(name: String): SimpleAttribute = copy(name = Some(name))
  override def withoutName: SimpleAttribute = copy(name = None)

  override def withIndicesRange(begin: Int, end: Int): SimpleAttribute =
    copy(indicesRange = Seq(begin, end))
  override def withIndicesRange(index: Int): SimpleAttribute = copy(indicesRange = Seq(index))

  override def withoutIndicesRange: SimpleAttribute = copy(indicesRange = Seq.empty)

  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))
    bldr.putLongArray(AttributeKeys.INDICES, indicesRange.toArray.map(_.toLong))
    values.foreach(v => bldr.putStringArray(AttributeKeys.VALUES, v.toArray))

    bldr.build()
  }
}

/**
 * :: DeveloperApi ::
 * A numeric attribute with optional summary statistics.
 * @param min optional min value.
 * @param max optional max value.
 * @param std optional standard deviation.
 * @param sparsity optional sparsity (ratio of zeros).
 */
@DeveloperApi
case class NumericAttr(
    name: Option[String] = None,
    indicesRange: Seq[Int] = Seq.empty,
    min: Option[Double] = None,
    max: Option[Double] = None,
    std: Option[Double] = None,
    sparsity: Option[Double] = None) extends SimpleAttribute {

  std.foreach { s =>
    require(s >= 0.0, s"Standard deviation cannot be negative but got $s.")
  }
  sparsity.foreach { s =>
    require(s >= 0.0 && s <= 1.0, s"Sparsity must be in [0, 1] but got $s.")
  }

  override val attrType: AttributeType = AttributeType.Numeric

  override def withName(name: String): SimpleAttribute = copy(name = Some(name))
  override def withoutName: SimpleAttribute = copy(name = None)

  override def withIndicesRange(begin: Int, end: Int): SimpleAttribute =
    copy(indicesRange = Seq(begin, end))
  override def withIndicesRange(index: Int): SimpleAttribute = copy(indicesRange = Seq(index))

  override def withoutIndicesRange: SimpleAttribute = copy(indicesRange = Seq.empty)

  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))
    bldr.putLongArray(AttributeKeys.INDICES, indicesRange.toArray.map(_.toLong))

    min.foreach(bldr.putDouble(AttributeKeys.MIN, _))
    max.foreach(bldr.putDouble(AttributeKeys.MAX, _))
    std.foreach(bldr.putDouble(AttributeKeys.STD, _))
    sparsity.foreach(bldr.putDouble(AttributeKeys.SPARSITY, _))

    bldr.build()
  }
}

/**
 * :: DeveloperApi ::
 * A numeric attribute with optional summary statistics.
 * @param attributes the attributes included in this vector column.
 */
@DeveloperApi
case class VectorAttr(
    name: Option[String] = None,
    attributes: Seq[SimpleAttribute] = Seq.empty) extends ComplexAttribute {

  override val attrType: AttributeType = AttributeType.Vector

  override def withName(name: String): VectorAttr = copy(name = Some(name))
  override def withoutName: VectorAttr = copy(name = None)

  override def withAttributes(attributes: Seq[SimpleAttribute]): VectorAttr =
    copy(attributes = attributes)
  override def withoutAttributes: VectorAttr = copy(attributes = Seq.empty)

  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))

    // Build the metadata of attributes included in this vector attribute.
    val attrMetadata = attributes.map { attr =>
      attr.toMetadata()
    }
    bldr.putMetadataArray(AttributeKeys.ATTRIBUTES, attrMetadata.toArray)

    bldr.build()
  }
}

@DeveloperApi
object NumericAttr extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): NumericAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, indicesRange) = loadCommonMetadata(metadata)

    val min = if (metadata.contains(MIN)) Some(metadata.getDouble(MIN)) else None
    val max = if (metadata.contains(MAX)) Some(metadata.getDouble(MAX)) else None
    val std = if (metadata.contains(STD)) Some(metadata.getDouble(STD)) else None
    val sparsity = if (metadata.contains(SPARSITY)) Some(metadata.getDouble(SPARSITY)) else None

    NumericAttr(name, indicesRange, min, max, std, sparsity)
  }
}

@DeveloperApi
object BinaryAttr extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): BinaryAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, indicesRange) = loadCommonMetadata(metadata)

    val values = if (metadata.contains(VALUES)) {
      Some(metadata.getStringArray(VALUES))
    } else {
      None
    }

    BinaryAttr(name, indicesRange, values)
  }
}

@DeveloperApi
object NominalAttr extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): NominalAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, indicesRange) = loadCommonMetadata(metadata)

    val isOrdinal = if (metadata.contains(ORDINAL)) Some(metadata.getBoolean(ORDINAL)) else None
    val values = if (metadata.contains(VALUES)) {
      Some(metadata.getStringArray(VALUES))
    } else {
      None
    }

    NominalAttr(name, indicesRange, isOrdinal, values)
  }
}

@DeveloperApi
object VectorAttr extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): VectorAttr = {
    val (name, _) = loadCommonMetadata(metadata)
    val attributes = if (metadata.contains(AttributeKeys.ATTRIBUTES)) {
      // `VectorAttr` can only contains `SimpleAttribute`.
      metadata.getMetadataArray(AttributeKeys.ATTRIBUTES).map { metadata =>
        MLAttributes.fromMetadata(metadata).asInstanceOf[SimpleAttribute]
      }.toSeq
    } else {
      Seq.empty
    }

    VectorAttr(name, attributes)
  }
}
