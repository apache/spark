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

import scala.annotation.varargs

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.sql.types.{DoubleType, NumericType, Metadata, MetadataBuilder, StructField}

/**
 * :: DeveloperApi ::
 * Abstract class for ML attributes.
 */
@DeveloperApi
@Since("1.4.0")
sealed abstract class Attribute extends Serializable {

  name.foreach { n =>
    require(n.nonEmpty, "Cannot have an empty string for name.")
  }
  index.foreach { i =>
    require(i >= 0, s"Index cannot be negative but got $i")
  }

  /** Attribute type. */
  @Since("1.4.0")
  def attrType: AttributeType

  /** Name of the attribute. None if it is not set. */
  @Since("1.4.0")
  def name: Option[String]

  /** Copy with a new name. */
  @Since("1.4.0")
  def withName(name: String): Attribute

  /** Copy without the name. */
  @Since("1.4.0")
  def withoutName: Attribute

  /** Index of the attribute. None if it is not set. */
  @Since("1.4.0")
  def index: Option[Int]

  /** Copy with a new index. */
  @Since("1.4.0")
  def withIndex(index: Int): Attribute

  /** Copy without the index. */
  @Since("1.4.0")
  def withoutIndex: Attribute

  /**
   * Tests whether this attribute is numeric, true for [[NumericAttribute]] and [[BinaryAttribute]].
   */
  @Since("1.4.0")
  def isNumeric: Boolean

  /**
   * Tests whether this attribute is nominal, true for [[NominalAttribute]] and [[BinaryAttribute]].
   */
  @Since("1.4.0")
  def isNominal: Boolean

  /**
   * Converts this attribute to [[Metadata]].
   * @param withType whether to include the type info
   */
  private[attribute] def toMetadataImpl(withType: Boolean): Metadata

  /**
   * Converts this attribute to [[Metadata]]. For numeric attributes, the type info is excluded to
   * save space, because numeric type is the default attribute type. For nominal and binary
   * attributes, the type info is included.
   */
  private[attribute] def toMetadataImpl(): Metadata = {
    if (attrType == AttributeType.Numeric) {
      toMetadataImpl(withType = false)
    } else {
      toMetadataImpl(withType = true)
    }
  }

  /** Converts to ML metadata with some existing metadata. */
  @Since("1.4.0")
  def toMetadata(existingMetadata: Metadata): Metadata = {
    new MetadataBuilder()
      .withMetadata(existingMetadata)
      .putMetadata(AttributeKeys.ML_ATTR, toMetadataImpl())
      .build()
  }

  /** Converts to ML metadata */
  @Since("1.4.0")
  def toMetadata(): Metadata = toMetadata(Metadata.empty)

  /**
   * Converts to a [[StructField]] with some existing metadata.
   * @param existingMetadata existing metadata to carry over
   */
  @Since("1.4.0")
  def toStructField(existingMetadata: Metadata): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(existingMetadata)
      .putMetadata(AttributeKeys.ML_ATTR, withoutName.withoutIndex.toMetadataImpl())
      .build()
    StructField(name.get, DoubleType, nullable = false, newMetadata)
  }

  /** Converts to a [[StructField]]. */
  @Since("1.4.0")
  def toStructField(): StructField = toStructField(Metadata.empty)

  @Since("1.4.0")
  override def toString: String = toMetadataImpl(withType = true).toString
}

/** Trait for ML attribute factories. */
private[attribute] trait AttributeFactory {

  /**
   * Creates an [[Attribute]] from a [[Metadata]] instance.
   */
  private[attribute] def fromMetadata(metadata: Metadata): Attribute

  /**
   * Creates an [[Attribute]] from a [[StructField]] instance, optionally preserving name.
   */
  private[ml] def decodeStructField(field: StructField, preserveName: Boolean): Attribute = {
    require(field.dataType.isInstanceOf[NumericType])
    val metadata = field.metadata
    val mlAttr = AttributeKeys.ML_ATTR
    if (metadata.contains(mlAttr)) {
      val attr = fromMetadata(metadata.getMetadata(mlAttr))
      if (preserveName) {
        attr
      } else {
        attr.withName(field.name)
      }
    } else {
      UnresolvedAttribute
    }
  }

  /**
   * Creates an [[Attribute]] from a [[StructField]] instance.
   */
  def fromStructField(field: StructField): Attribute = decodeStructField(field, false)
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
@Since("1.4.0")
object Attribute extends AttributeFactory {

  private[attribute] override def fromMetadata(metadata: Metadata): Attribute = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val attrType = if (metadata.contains(TYPE)) {
      metadata.getString(TYPE)
    } else {
      AttributeType.Numeric.name
    }
    getFactory(attrType).fromMetadata(metadata)
  }

  /** Gets the attribute factory given the attribute type name. */
  private def getFactory(attrType: String): AttributeFactory = {
    if (attrType == AttributeType.Numeric.name) {
      NumericAttribute
    } else if (attrType == AttributeType.Nominal.name) {
      NominalAttribute
    } else if (attrType == AttributeType.Binary.name) {
      BinaryAttribute
    } else {
      throw new IllegalArgumentException(s"Cannot recognize type $attrType.")
    }
  }
}


/**
 * :: DeveloperApi ::
 * A numeric attribute with optional summary statistics.
 * @param name optional name
 * @param index optional index
 * @param min optional min value
 * @param max optional max value
 * @param std optional standard deviation
 * @param sparsity optional sparsity (ratio of zeros)
 */
@DeveloperApi
@Since("1.4.0")
class NumericAttribute private[ml] (
    @Since("1.4.0") override val name: Option[String] = None,
    @Since("1.4.0") override val index: Option[Int] = None,
    @Since("1.4.0") val min: Option[Double] = None,
    @Since("1.4.0") val max: Option[Double] = None,
    @Since("1.4.0") val std: Option[Double] = None,
    @Since("1.4.0") val sparsity: Option[Double] = None) extends Attribute {

  std.foreach { s =>
    require(s >= 0.0, s"Standard deviation cannot be negative but got $s.")
  }
  sparsity.foreach { s =>
    require(s >= 0.0 && s <= 1.0, s"Sparsity must be in [0, 1] but got $s.")
  }

  @Since("1.4.0")
  override def attrType: AttributeType = AttributeType.Numeric

  @Since("1.4.0")
  override def withName(name: String): NumericAttribute = copy(name = Some(name))

  @Since("1.4.0")
  override def withoutName: NumericAttribute = copy(name = None)

  @Since("1.4.0")
  override def withIndex(index: Int): NumericAttribute = copy(index = Some(index))

  @Since("1.4.0")
  override def withoutIndex: NumericAttribute = copy(index = None)

  /** Copy with a new min value. */
  @Since("1.4.0")
  def withMin(min: Double): NumericAttribute = copy(min = Some(min))

  /** Copy without the min value. */
  @Since("1.4.0")
  def withoutMin: NumericAttribute = copy(min = None)


  /** Copy with a new max value. */
  @Since("1.4.0")
  def withMax(max: Double): NumericAttribute = copy(max = Some(max))

  /** Copy without the max value. */
  @Since("1.4.0")
  def withoutMax: NumericAttribute = copy(max = None)

  /** Copy with a new standard deviation. */
  @Since("1.4.0")
  def withStd(std: Double): NumericAttribute = copy(std = Some(std))

  /** Copy without the standard deviation. */
  @Since("1.4.0")
  def withoutStd: NumericAttribute = copy(std = None)

  /** Copy with a new sparsity. */
  @Since("1.4.0")
  def withSparsity(sparsity: Double): NumericAttribute = copy(sparsity = Some(sparsity))

  /** Copy without the sparsity. */
  @Since("1.4.0")
  def withoutSparsity: NumericAttribute = copy(sparsity = None)

  /** Copy without summary statistics. */

  @Since("1.4.0")
  def withoutSummary: NumericAttribute = copy(min = None, max = None, std = None, sparsity = None)

  @Since("1.4.0")
  override def isNumeric: Boolean = true

  @Since("1.4.0")
  override def isNominal: Boolean = false

  /** Convert this attribute to metadata. */
  override private[attribute] def toMetadataImpl(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val bldr = new MetadataBuilder()
    if (withType) bldr.putString(TYPE, attrType.name)
    name.foreach(bldr.putString(NAME, _))
    index.foreach(bldr.putLong(INDEX, _))
    min.foreach(bldr.putDouble(MIN, _))
    max.foreach(bldr.putDouble(MAX, _))
    std.foreach(bldr.putDouble(STD, _))
    sparsity.foreach(bldr.putDouble(SPARSITY, _))
    bldr.build()
  }

  /** Creates a copy of this attribute with optional changes. */
  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      min: Option[Double] = min,
      max: Option[Double] = max,
      std: Option[Double] = std,
      sparsity: Option[Double] = sparsity): NumericAttribute = {
    new NumericAttribute(name, index, min, max, std, sparsity)
  }

  @Since("1.4.0")
  override def equals(other: Any): Boolean = {
    other match {
      case o: NumericAttribute =>
        (name == o.name) &&
          (index == o.index) &&
          (min == o.min) &&
          (max == o.max) &&
          (std == o.std) &&
          (sparsity == o.sparsity)
      case _ =>
        false
    }
  }

  @Since("1.4.0")
  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + min.hashCode
    sum = 37 * sum + max.hashCode
    sum = 37 * sum + std.hashCode
    sum = 37 * sum + sparsity.hashCode
    sum
  }
}

/**
 * :: DeveloperApi ::
 * Factory methods for numeric attributes.
 */
@DeveloperApi
@Since("1.4.0")
object NumericAttribute extends AttributeFactory {

  /** The default numeric attribute. */
  @Since("1.4.0")
  val defaultAttr: NumericAttribute = new NumericAttribute

  private[attribute] override def fromMetadata(metadata: Metadata): NumericAttribute = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val name = if (metadata.contains(NAME)) Some(metadata.getString(NAME)) else None
    val index = if (metadata.contains(INDEX)) Some(metadata.getLong(INDEX).toInt) else None
    val min = if (metadata.contains(MIN)) Some(metadata.getDouble(MIN)) else None
    val max = if (metadata.contains(MAX)) Some(metadata.getDouble(MAX)) else None
    val std = if (metadata.contains(STD)) Some(metadata.getDouble(STD)) else None
    val sparsity = if (metadata.contains(SPARSITY)) Some(metadata.getDouble(SPARSITY)) else None
    new NumericAttribute(name, index, min, max, std, sparsity)
  }
}

/**
 * :: DeveloperApi ::
 * A nominal attribute.
 * @param name optional name
 * @param index optional index
 * @param isOrdinal whether this attribute is ordinal (optional)
 * @param numValues optional number of values. At most one of `numValues` and `values` can be
 *                  defined.
 * @param values optional values. At most one of `numValues` and `values` can be defined.
 */
@DeveloperApi
@Since("1.4.0")
class NominalAttribute private[ml] (
    @Since("1.4.0") override val name: Option[String] = None,
    @Since("1.4.0") override val index: Option[Int] = None,
    @Since("1.4.0") val isOrdinal: Option[Boolean] = None,
    @Since("1.4.0") val numValues: Option[Int] = None,
    @Since("1.4.0") val values: Option[Array[String]] = None) extends Attribute {

  numValues.foreach { n =>
    require(n >= 0, s"numValues cannot be negative but got $n.")
  }
  require(!(numValues.isDefined && values.isDefined),
    "Cannot have both numValues and values defined.")

  @Since("1.4.0")
  override def attrType: AttributeType = AttributeType.Nominal

  @Since("1.4.0")
  override def isNumeric: Boolean = false

  @Since("1.4.0")
  override def isNominal: Boolean = true

  private lazy val valueToIndex: Map[String, Int] = {
    values.map(_.zipWithIndex.toMap).getOrElse(Map.empty)
  }

  /** Index of a specific value. */
  @Since("1.4.0")
  def indexOf(value: String): Int = {
    valueToIndex(value)
  }

  /** Tests whether this attribute contains a specific value. */
  @Since("1.4.0")
  def hasValue(value: String): Boolean = valueToIndex.contains(value)

  /** Gets a value given its index. */
  @Since("1.4.0")
  def getValue(index: Int): String = values.get(index)

  @Since("1.4.0")
  override def withName(name: String): NominalAttribute = copy(name = Some(name))

  @Since("1.4.0")
  override def withoutName: NominalAttribute = copy(name = None)

  @Since("1.4.0")
  override def withIndex(index: Int): NominalAttribute = copy(index = Some(index))

  @Since("1.4.0")
  override def withoutIndex: NominalAttribute = copy(index = None)

  /** Copy with new values and empty `numValues`. */
  @Since("1.4.0")
  def withValues(values: Array[String]): NominalAttribute = {
    copy(numValues = None, values = Some(values))
  }

  /** Copy with new values and empty `numValues`. */
  @varargs
  @Since("1.4.0")
  def withValues(first: String, others: String*): NominalAttribute = {
    copy(numValues = None, values = Some((first +: others).toArray))
  }

  /** Copy without the values. */
  @Since("1.4.0")
  def withoutValues: NominalAttribute = {
    copy(values = None)
  }

  /** Copy with a new `numValues` and empty `values`. */
  @Since("1.4.0")
  def withNumValues(numValues: Int): NominalAttribute = {
    copy(numValues = Some(numValues), values = None)
  }

  /** Copy without the `numValues`. */
  @Since("1.4.0")
  def withoutNumValues: NominalAttribute = copy(numValues = None)

  /**
   * Get the number of values, either from `numValues` or from `values`.
   * Return None if unknown.
   */
  @Since("1.4.0")
  def getNumValues: Option[Int] = {
    if (numValues.nonEmpty) {
      numValues
    } else if (values.nonEmpty) {
      Some(values.get.length)
    } else {
      None
    }
  }

  /** Creates a copy of this attribute with optional changes. */
  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      isOrdinal: Option[Boolean] = isOrdinal,
      numValues: Option[Int] = numValues,
      values: Option[Array[String]] = values): NominalAttribute = {
    new NominalAttribute(name, index, isOrdinal, numValues, values)
  }

  override private[attribute] def toMetadataImpl(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val bldr = new MetadataBuilder()
    if (withType) bldr.putString(TYPE, attrType.name)
    name.foreach(bldr.putString(NAME, _))
    index.foreach(bldr.putLong(INDEX, _))
    isOrdinal.foreach(bldr.putBoolean(ORDINAL, _))
    numValues.foreach(bldr.putLong(NUM_VALUES, _))
    values.foreach(v => bldr.putStringArray(VALUES, v))
    bldr.build()
  }

  @Since("1.4.0")
  override def equals(other: Any): Boolean = {
    other match {
      case o: NominalAttribute =>
        (name == o.name) &&
          (index == o.index) &&
          (isOrdinal == o.isOrdinal) &&
          (numValues == o.numValues) &&
          (values.map(_.toSeq) == o.values.map(_.toSeq))
      case _ =>
        false
    }
  }

  @Since("1.4.0")
  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + isOrdinal.hashCode
    sum = 37 * sum + numValues.hashCode
    sum = 37 * sum + values.map(_.toSeq).hashCode
    sum
  }
}

/**
 * :: DeveloperApi ::
 * Factory methods for nominal attributes.
 */
@DeveloperApi
@Since("1.4.0")
object NominalAttribute extends AttributeFactory {

  /** The default nominal attribute. */
  @Since("1.4.0")
  final val defaultAttr: NominalAttribute = new NominalAttribute

  private[attribute] override def fromMetadata(metadata: Metadata): NominalAttribute = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val name = if (metadata.contains(NAME)) Some(metadata.getString(NAME)) else None
    val index = if (metadata.contains(INDEX)) Some(metadata.getLong(INDEX).toInt) else None
    val isOrdinal = if (metadata.contains(ORDINAL)) Some(metadata.getBoolean(ORDINAL)) else None
    val numValues =
      if (metadata.contains(NUM_VALUES)) Some(metadata.getLong(NUM_VALUES).toInt) else None
    val values =
      if (metadata.contains(VALUES)) Some(metadata.getStringArray(VALUES)) else None
    new NominalAttribute(name, index, isOrdinal, numValues, values)
  }
}

/**
 * :: DeveloperApi ::
 * A binary attribute.
 * @param name optional name
 * @param index optional index
 * @param values optionla values. If set, its size must be 2.
 */
@DeveloperApi
@Since("1.4.0")
class BinaryAttribute private[ml] (
    @Since("1.4.0") override val name: Option[String] = None,
    @Since("1.4.0") override val index: Option[Int] = None,
    @Since("1.4.0") val values: Option[Array[String]] = None)
  extends Attribute {

  values.foreach { v =>
    require(v.length == 2, s"Number of values must be 2 for a binary attribute but got ${v.toSeq}.")
  }

  @Since("1.4.0")
  override def attrType: AttributeType = AttributeType.Binary

  @Since("1.4.0")
  override def isNumeric: Boolean = true

  @Since("1.4.0")
  override def isNominal: Boolean = true

  @Since("1.4.0")
  override def withName(name: String): BinaryAttribute = copy(name = Some(name))

  @Since("1.4.0")
  override def withoutName: BinaryAttribute = copy(name = None)

  @Since("1.4.0")
  override def withIndex(index: Int): BinaryAttribute = copy(index = Some(index))

  @Since("1.4.0")
  override def withoutIndex: BinaryAttribute = copy(index = None)

  /**
   * Copy with new values.
   * @param negative name for negative
   * @param positive name for positive
   */
  @Since("1.4.0")
  def withValues(negative: String, positive: String): BinaryAttribute =
    copy(values = Some(Array(negative, positive)))

  /** Copy without the values. */
  @Since("1.4.0")
  def withoutValues: BinaryAttribute = copy(values = None)

  /** Creates a copy of this attribute with optional changes. */
  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      values: Option[Array[String]] = values): BinaryAttribute = {
    new BinaryAttribute(name, index, values)
  }

  override private[attribute] def toMetadataImpl(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val bldr = new MetadataBuilder
    if (withType) bldr.putString(TYPE, attrType.name)
    name.foreach(bldr.putString(NAME, _))
    index.foreach(bldr.putLong(INDEX, _))
    values.foreach(v => bldr.putStringArray(VALUES, v))
    bldr.build()
  }

  @Since("1.4.0")
  override def equals(other: Any): Boolean = {
    other match {
      case o: BinaryAttribute =>
        (name == o.name) &&
          (index == o.index) &&
          (values.map(_.toSeq) == o.values.map(_.toSeq))
      case _ =>
        false
    }
  }

  @Since("1.4.0")
  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + values.map(_.toSeq).hashCode
    sum
  }
}

/**
 * :: DeveloperApi ::
 * Factory methods for binary attributes.
 */
@DeveloperApi
@Since("1.4.0")
object BinaryAttribute extends AttributeFactory {

  /** The default binary attribute. */
  @Since("1.4.0")
  final val defaultAttr: BinaryAttribute = new BinaryAttribute

  private[attribute] override def fromMetadata(metadata: Metadata): BinaryAttribute = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val name = if (metadata.contains(NAME)) Some(metadata.getString(NAME)) else None
    val index = if (metadata.contains(INDEX)) Some(metadata.getLong(INDEX).toInt) else None
    val values =
      if (metadata.contains(VALUES)) Some(metadata.getStringArray(VALUES)) else None
    new BinaryAttribute(name, index, values)
  }
}

/**
 * :: DeveloperApi ::
 * An unresolved attribute.
 */
@DeveloperApi
@Since("1.5.1")
object UnresolvedAttribute extends Attribute {

  @Since("1.5.1")
  override def attrType: AttributeType = AttributeType.Unresolved

  @Since("1.5.1")
  override def withIndex(index: Int): Attribute = this

  @Since("1.5.1")
  override def isNumeric: Boolean = false

  @Since("1.5.1")
  override def withoutIndex: Attribute = this

  @Since("1.5.1")
  override def isNominal: Boolean = false

  @Since("1.5.1")
  override def name: Option[String] = None

  override private[attribute] def toMetadataImpl(withType: Boolean): Metadata = {
    Metadata.empty
  }

  @Since("1.5.1")
  override def withoutName: Attribute = this

  @Since("1.5.1")
  override def index: Option[Int] = None

  @Since("1.5.1")
  override def withName(name: String): Attribute = this

}
