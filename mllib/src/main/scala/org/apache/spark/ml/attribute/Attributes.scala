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
    names: Seq[String] = Seq.empty,
    indices: Seq[Int] = Seq.empty,
    isOrdinal: Option[Boolean] = None,
    values: Option[Array[String]] = None) extends SimpleAttribute {

  override val attrType: AttributeType = AttributeType.Numeric

  override def withName(name: String): SimpleAttribute = copy(name = Some(name))
  override def withoutName: SimpleAttribute = copy(name = None)

  override def withIndices(indices: Seq[Int]): SimpleAttribute = copy(indices = indices)
  override def withoutIndices: SimpleAttribute = copy(indices = Seq.empty)

  override def withNames(names: Seq[String]): SimpleAttribute = copy(names = names)
  override def withoutNames: SimpleAttribute = copy(names = Seq.empty)

  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))
    bldr.putStringArray(AttributeKeys.NAMES, names.toArray)
    bldr.putLongArray(AttributeKeys.INDICES, indices.toArray.map(_.toLong))
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
    names: Seq[String] = Seq.empty,
    indices: Seq[Int] = Seq.empty,
    values: Option[Array[String]] = None) extends SimpleAttribute {

  values.foreach { v =>
    require(v.length == 2, s"Number of values must be 2 for a binary attribute but got ${v.toSeq}.")
  }

  override val attrType: AttributeType = AttributeType.Binary

  override def withName(name: String): SimpleAttribute = copy(name = Some(name))
  override def withoutName: SimpleAttribute = copy(name = None)

  override def withIndices(indices: Seq[Int]): SimpleAttribute = copy(indices = indices)
  override def withoutIndices: SimpleAttribute = copy(indices = Seq.empty)

  override def withNames(names: Seq[String]): SimpleAttribute = copy(names = names)
  override def withoutNames: SimpleAttribute = copy(names = Seq.empty)

  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))
    bldr.putStringArray(AttributeKeys.NAMES, names.toArray)
    bldr.putLongArray(AttributeKeys.INDICES, indices.toArray.map(_.toLong))
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
    names: Seq[String] = Seq.empty,
    indices: Seq[Int] = Seq.empty,
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

  override def withIndices(indices: Seq[Int]): SimpleAttribute = copy(indices = indices)
  override def withoutIndices: SimpleAttribute = copy(indices = Seq.empty)

  override def withNames(names: Seq[String]): SimpleAttribute = copy(names = names)
  override def withoutNames: SimpleAttribute = copy(names = Seq.empty)


  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))
    bldr.putStringArray(AttributeKeys.NAMES, names.toArray)
    bldr.putLongArray(AttributeKeys.INDICES, indices.toArray.map(_.toLong))

    min.foreach(bldr.putDouble(AttributeKeys.MIN, _))
    max.foreach(bldr.putDouble(AttributeKeys.MAX, _))
    std.foreach(bldr.putDouble(AttributeKeys.STD, _))
    sparsity.foreach(bldr.putDouble(AttributeKeys.SPARSITY, _))

    bldr.build()
  }
}

case class ComplexAttr(
    name: Option[String] = None,
    attributes: Seq[SimpleAttribute] = Seq.empty) extends ComplexAttribute {

  override val attrType: AttributeType = AttributeType.Complex

  override def withName(name: String): ComplexAttribute = copy(name = Some(name))
  override def withoutName: ComplexAttribute = copy(name = None)

  override def withAttributes(attributes: Seq[SimpleAttribute]): ComplexAttribute =
    copy(attributes = attributes)
  override def withoutAttributes: ComplexAttribute = copy(attributes = Seq.empty)

  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))

    // Build the metadata of attributes included in this complex attribute.
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

    val (name, names, indices) = loadCommonMetadata(metadata)

    val min = if (metadata.contains(MIN)) Some(metadata.getDouble(MIN)) else None
    val max = if (metadata.contains(MAX)) Some(metadata.getDouble(MAX)) else None
    val std = if (metadata.contains(STD)) Some(metadata.getDouble(STD)) else None
    val sparsity = if (metadata.contains(SPARSITY)) Some(metadata.getDouble(SPARSITY)) else None

    NumericAttr(name, names, indices, min, max, std, sparsity)
  }
}

@DeveloperApi
object BinaryAttr extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): BinaryAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, names, indices) = loadCommonMetadata(metadata)

    val values = if (metadata.contains(VALUES)) {
      Some(metadata.getStringArray(VALUES))
    } else {
      None
    }

    BinaryAttr(name, names, indices, values)
  }
}

@DeveloperApi
object NominalAttr extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): NominalAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, names, indices) = loadCommonMetadata(metadata)

    val isOrdinal = if (metadata.contains(ORDINAL)) Some(metadata.getBoolean(ORDINAL)) else None
    val values = if (metadata.contains(VALUES)) {
      Some(metadata.getStringArray(VALUES))
    } else {
      None
    }

    NominalAttr(name, names, indices, isOrdinal, values)
  }
}

@DeveloperApi
object ComplexAttr extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): ComplexAttr = {
    val (name, _, _) = loadCommonMetadata(metadata)
    val attributes = if (metadata.contains(AttributeKeys.ATTRIBUTES)) {
      // `ComplexAttr` can only contains `SimpleAttribute`.
      metadata.getMetadataArray(AttributeKeys.ATTRIBUTES).map { metadata =>
        MLAttributes.fromMetadata(metadata).asInstanceOf[SimpleAttribute]
      }.toSeq
    } else {
      Seq.empty
    }

    ComplexAttr(name, attributes)
  }
}
