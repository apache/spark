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

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.types.{BooleanType, DoubleType, Metadata, MetadataBuilder, NumericType, StructField}

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

  override val attrType: AttributeType = AttributeType.Nominal

  override def withName(name: String): NominalAttr = copy(name = Some(name))
  override def withoutName: NominalAttr = copy(name = None)

  override def withIndicesRange(begin: Int, end: Int): NominalAttr =
    copy(indicesRange = Seq(begin, end))
  override def withIndicesRange(index: Int): NominalAttr = copy(indicesRange = Seq(index))
  override def withIndicesRange(indices: Seq[Int]): NominalAttr =
    copy(indicesRange = indices)

  override def withoutIndicesRange: NominalAttr = copy(indicesRange = Seq.empty)

  // Java-friendly APIs to set up attribute properties.
  def withIsOrdinal(isOrdinal: Boolean): NominalAttr = copy(isOrdinal = Some(isOrdinal))
  def withValues(values: Array[String]): NominalAttr = copy(values = Some(values))

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

  override def withName(name: String): BinaryAttr = copy(name = Some(name))
  override def withoutName: BinaryAttr = copy(name = None)

  override def withIndicesRange(begin: Int, end: Int): BinaryAttr =
    copy(indicesRange = Seq(begin, end))
  override def withIndicesRange(index: Int): BinaryAttr = copy(indicesRange = Seq(index))
  override def withIndicesRange(indices: Seq[Int]): BinaryAttr =
    copy(indicesRange = indices)

  override def withoutIndicesRange: BinaryAttr = copy(indicesRange = Seq.empty)

  // Java-friendly APIs to set up attribute properties.
  def withValues(values: Array[String]): BinaryAttr = copy(values = Some(values))

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

  override def withName(name: String): NumericAttr = copy(name = Some(name))
  override def withoutName: NumericAttr = copy(name = None)

  override def withIndicesRange(begin: Int, end: Int): NumericAttr =
    copy(indicesRange = Seq(begin, end))
  override def withIndicesRange(index: Int): NumericAttr = copy(indicesRange = Seq(index))
  override def withIndicesRange(indices: Seq[Int]): NumericAttr =
    copy(indicesRange = indices)

  override def withoutIndicesRange: NumericAttr = copy(indicesRange = Seq.empty)

  // Java-friendly APIs to set up attribute properties.
  def withMin(min: Double): NumericAttr = copy(min = Some(min))
  def withMax(max: Double): NumericAttr = copy(max = Some(max))
  def withStd(std: Double): NumericAttr = copy(std = Some(std))
  def withSparsity(sparsity: Double): NumericAttr = copy(sparsity = Some(sparsity))

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
 * An attribute that can contain other attributes, represents a ML vector column.
 * The inner attributes can be accessed by using indices in the vector. The names of the
 * inner attributes are meaningless and won't be serialized.
 *
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
    copy(attributes = attributes.map(_.withoutName))
  override def withoutAttributes: VectorAttr = copy(attributes = Seq.empty)

  override def getAttribute(idx: Int): BaseAttribute = {
    attributes.find { attr =>
      attr.indicesRange match {
        case Seq(exactIdx) if exactIdx == idx => true
        case Seq(from, to) if from <= idx && idx <= to => true
        case _ => false
      }
    }.getOrElse(UnresolvedMLAttribute).withoutName
  }

  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))

    // Build the metadata of attributes included in this vector attribute.
    val attrMetadata = attributes.map { attr =>
      attr.withoutName.toMetadata()
    }
    bldr.putMetadataArray(AttributeKeys.ATTRIBUTES, attrMetadata.toArray)

    bldr.build()
  }
}

/**
 * :: DeveloperApi ::
 * The factory object for `NumericAttr` used to load the attribute from `Metadata`.
 */
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

/**
 * :: DeveloperApi ::
 * The factory object for `BinaryAttr` used to load the attribute from `Metadata`.
 */
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

/**
 * :: DeveloperApi ::
 * The factory object for `NominalAttr` used to load the attribute from `Metadata`.
 */
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

/**
 * :: DeveloperApi ::
 * The factory object for `VectorAttr` used to load the attribute from `Metadata`.
 */
@DeveloperApi
object VectorAttr extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): VectorAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, _) = loadCommonMetadata(metadata)
    val attributes = if (metadata.contains(ATTRIBUTES)) {
      // `VectorAttr` can only contains `SimpleAttribute`.
      metadata.getMetadataArray(ATTRIBUTES).map { metadata =>
        MLAttributes.fromMetadata(metadata.getMetadata(ML_ATTRV2)).asInstanceOf[SimpleAttribute]
      }.toSeq
    } else {
      Seq.empty
    }

    VectorAttr(name, attributes)
  }
}

/**
 * :: DeveloperApi ::
 * Builder used to build ML vector attributes from `StructField`s.
 */
@DeveloperApi
object VectorAttrBuilder {
  // Whether two attributes are the same after dropping their names and indices.
  private def sameAttr(attr1: SimpleAttribute, attr2: SimpleAttribute): Boolean = {
    attr1.withoutIndicesRange.withoutName == attr2.withoutIndicesRange.withoutName
  }

  /**
   * Given a sequence of `StructField`, a `AttrBuilder` should be able to build a `VectorAttr`.
   *
   * @param numAttrsInVectors The number of attributes in vector columns. For non-vector columns,
   *                            the number is zero.
   */
  def buildAttr(fields: Seq[StructField], numAttrsInVectors: Seq[Int]): VectorAttr = {
    require(fields.length == numAttrsInVectors.length,
      "`numAttrsInVectors`'s length should be the same with `fields`'s length")

    var currAttr: Option[SimpleAttribute] = None
    var fieldIdx = 0
    val innerAttributes = mutable.ArrayBuffer[SimpleAttribute]()

    fields.zipWithIndex.foreach { case (field, idx) =>
      val attr = MLAttributes.fromStructField(field, preserveName = false)
      field.dataType match {
        case DoubleType =>
          val newAttr = if (attr == UnresolvedMLAttribute) {
            // Assume numeric attribute.
            // Or just use `UnresolvedMLAttribute`?
            NumericAttr().withIndicesRange(fieldIdx)
          } else {
            // Double column can only have `SimpleAttribute`.
            attr.asInstanceOf[SimpleAttribute].withIndicesRange(fieldIdx).withoutName
          }
          // If this attribute is basically the same with previous one, we combine them together.
          if (currAttr.isDefined && sameAttr(currAttr.get, newAttr)) {
            currAttr = currAttr.map { attr =>
              attr.withIndicesRange(attr.indicesRange(0), newAttr.indicesRange(0))
            }
          } else {
            currAttr.map(innerAttributes += _)
            currAttr = Some(newAttr)
          }
          fieldIdx += 1
        case _: NumericType | BooleanType =>
          require(attr == UnresolvedMLAttribute, "numeric/boolean column shouldn't have attribute.")

          // Assume numeric attribute.
          // Note: should we assume `UnresolvedMLAttribute` for this kind of columns?
          val newAttr = NumericAttr().withIndicesRange(fieldIdx)
          // If this attribute is basically the same with previous one, we combine them together.
          if (currAttr.isDefined && sameAttr(currAttr.get, newAttr)) {
            currAttr = currAttr.map { attr =>
              attr.withIndicesRange(attr.indicesRange(0), newAttr.indicesRange(0))
            }
          } else {
            currAttr.map(innerAttributes += _)
            currAttr = Some(newAttr)
          }
          fieldIdx += 1
        case _: VectorUDT =>
          currAttr.map(innerAttributes += _)
          currAttr = None

          // If there is an attribute for this vector column.
          if (attr != UnresolvedMLAttribute) {
            val vectorAttr = attr.asInstanceOf[ComplexAttribute]
            fieldIdx = vectorAttr.attributes.map { a =>
              val innerAttr = if (a.name.isDefined) {
                a.withoutName
              } else {
                a
              }
              // Rebase the inner attribute indices.
              val indices = innerAttr.indicesRange
              innerAttributes += innerAttr.withIndicesRange(indices.map(_ + fieldIdx))
              indices(indices.length - 1) + fieldIdx
            }.max + 1
          } else {
            // If this vector has no metadata, we need to add up the number of attributes in the
            // vector, so the next field index can be correct.
            fieldIdx += numAttrsInVectors(idx)
          }
      }
    }
    currAttr.map(innerAttributes += _)
    VectorAttr(name = None, attributes = innerAttributes.toSeq)
  }
}
