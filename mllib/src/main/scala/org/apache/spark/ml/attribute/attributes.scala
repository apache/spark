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

import org.apache.spark.sql.types.{DoubleType, Metadata, MetadataBuilder, StructField}

/**
 * Abstract class for ML attributes.
 */
sealed abstract class Attribute extends Serializable {

  name.foreach { n =>
    require(n.nonEmpty, "Cannot have an empty string for name.")
  }
  index.foreach { i =>
    require(i >= 0, s"Index cannot be negative but got $i")
  }

  /** Attribute type. */
  def attrType: AttributeType

  /** Name of the attribute. None if it is not set. */
  def name: Option[String]

  /** Copy with a new name. */
  def withName(name: String): Attribute

  /** Copy without the name. */
  def withoutName: Attribute

  /** Index of the attribute. None if it is not set. */
  def index: Option[Int]

  /** Copy with a new index. */
  def withIndex(index: Int): Attribute

  /** Copy without the index. */
  def withoutIndex: Attribute

  /**
   * Tests whether this attribute is numeric, true for [[NumericAttribute]] and [[BinaryAttribute]].
   */
  def isNumeric: Boolean

  /**
   * Tests whether this attribute is nominal, true for [[NominalAttribute]] and [[BinaryAttribute]].
   */
  def isNominal: Boolean

  /**
   * Converts this attribute to [[Metadata]].
   * @param withType whether to include the type info
   */
  private[attribute] def toMetadata(withType: Boolean): Metadata

  /**
   * Converts this attribute to [[Metadata]]. For numeric attributes, the type info is excluded to
   * save space, because numeric type is the default attribute type. For nominal and binary
   * attributes, the type info is included.
   */
  private[attribute] def toMetadata(): Metadata = {
    if (attrType == AttributeType.Numeric) {
      toMetadata(withType = false)
    } else {
      toMetadata(withType = true)
    }
  }

  /**
   * Converts to a [[StructField]] with some existing metadata.
   * @param existingMetadata existing metadata to carry over
   */
  def toStructField(existingMetadata: Metadata): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(existingMetadata)
      .putMetadata(AttributeKeys.ML_ATTR, withoutName.withoutIndex.toMetadata())
      .build()
    StructField(name.get, DoubleType, nullable = false, newMetadata)
  }

  /** Converts to a [[StructField]]. */
  def toStructField(): StructField = toStructField(Metadata.empty)

  override def toString: String = toMetadata(withType = true).toString
}

/** Trait for ML attribute factories. */
private[attribute] trait AttributeFactory {

  /**
   * Creates an [[Attribute]] from a [[Metadata]] instance.
   */
  private[attribute] def fromMetadata(metadata: Metadata): Attribute

  /**
   * Creates an [[Attribute]] from a [[StructField]] instance.
   */
  def fromStructField(field: StructField): Attribute = {
    require(field.dataType == DoubleType)
    fromMetadata(field.metadata.getMetadata(AttributeKeys.ML_ATTR)).withName(field.name)
  }
}

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
 * A numeric attribute with optional summary statistics.
 * @param name optional name
 * @param index optional index
 * @param min optional min value
 * @param max optional max value
 * @param std optional standard deviation
 * @param sparsity optional sparsity (ratio of zeros)
 */
class NumericAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    val min: Option[Double] = None,
    val max: Option[Double] = None,
    val std: Option[Double] = None,
    val sparsity: Option[Double] = None) extends Attribute {

  std.foreach { s =>
    require(s >= 0.0, s"Standard deviation cannot be negative but got $s.")
  }
  sparsity.foreach { s =>
    require(s >= 0.0 && s <= 1.0, s"Sparsity must be in [0, 1] but got $s.")
  }

  override def attrType: AttributeType = AttributeType.Numeric

  override def withName(name: String): NumericAttribute = copy(name = Some(name))
  override def withoutName: NumericAttribute = copy(name = None)

  override def withIndex(index: Int): NumericAttribute = copy(index = Some(index))
  override def withoutIndex: NumericAttribute = copy(index = None)

  /** Copy with a new min value. */
  def withMin(min: Double): NumericAttribute = copy(min = Some(min))

  /** Copy without the min value. */
  def withoutMin: NumericAttribute = copy(min = None)


  /** Copy with a new max value. */
  def withMax(max: Double): NumericAttribute = copy(max = Some(max))

  /** Copy without the max value. */
  def withoutMax: NumericAttribute = copy(max = None)

  /** Copy with a new standard deviation. */
  def withStd(std: Double): NumericAttribute = copy(std = Some(std))

  /** Copy without the standard deviation. */
  def withoutStd: NumericAttribute = copy(std = None)

  /** Copy with a new sparsity. */
  def withSparsity(sparsity: Double): NumericAttribute = copy(sparsity = Some(sparsity))

  /** Copy without the sparsity. */
  def withoutSparsity: NumericAttribute = copy(sparsity = None)

  /** Copy without summary statistics. */
  def withoutSummary: NumericAttribute = copy(min = None, max = None, std = None, sparsity = None)

  override def isNumeric: Boolean = true

  override def isNominal: Boolean = false

  /** Convert this attribute to metadata. */
  private[attribute] override def toMetadata(withType: Boolean): Metadata = {
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
 * Factory methods for numeric attributes.
 */
object NumericAttribute extends AttributeFactory {

  /** The default numeric attribute. */
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
 * A nominal attribute.
 * @param name optional name
 * @param index optional index
 * @param isOrdinal whether this attribute is ordinal (optional)
 * @param numValues optional number of values. At most one of `numValues` and `values` can be
 *                  defined.
 * @param values optional values. At most one of `numValues` and `values` can be defined.
 */
class NominalAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    val isOrdinal: Option[Boolean] = None,
    val numValues: Option[Int] = None,
    val values: Option[Array[String]] = None) extends Attribute {

  numValues.foreach { n =>
    require(n >= 0, s"numValues cannot be negative but got $n.")
  }
  require(!(numValues.isDefined && values.isDefined),
    "Cannot have both numValues and values defined.")

  override def attrType: AttributeType = AttributeType.Nominal

  override def isNumeric: Boolean = false

  override def isNominal: Boolean = true

  private lazy val valueToIndex: Map[String, Int] = {
    values.map(_.zipWithIndex.toMap).getOrElse(Map.empty)
  }

  /** Index of a specific value. */
  def indexOf(value: String): Int = {
    valueToIndex(value)
  }

  /** Tests whether this attribute contains a specific value. */
  def hasValue(value: String): Boolean = valueToIndex.contains(value)

  /** Gets a value given its index. */
  def getValue(index: Int): String = values.get(index)

  override def withName(name: String): NominalAttribute = copy(name = Some(name))
  override def withoutName: NominalAttribute = copy(name = None)

  override def withIndex(index: Int): NominalAttribute = copy(index = Some(index))
  override def withoutIndex: NominalAttribute = copy(index = None)

  /** Copy with new values and empty `numValues`. */
  def withValues(values: Array[String]): NominalAttribute = {
    copy(numValues = None, values = Some(values))
  }

  /** Copy with new values and empty `numValues`. */
  @varargs
  def withValues(first: String, others: String*): NominalAttribute = {
    copy(numValues = None, values = Some((first +: others).toArray))
  }

  /** Copy without the values. */
  def withoutValues: NominalAttribute = {
    copy(values = None)
  }

  /** Copy with a new `numValues` and empty `values`. */
  def withNumValues(numValues: Int): NominalAttribute = {
    copy(numValues = Some(numValues), values = None)
  }

  /** Copy without the `numValues`. */
  def withoutNumValues: NominalAttribute = copy(numValues = None)

  /** Creates a copy of this attribute with optional changes. */
  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      isOrdinal: Option[Boolean] = isOrdinal,
      numValues: Option[Int] = numValues,
      values: Option[Array[String]] = values): NominalAttribute = {
    new NominalAttribute(name, index, isOrdinal, numValues, values)
  }

  private[attribute] override def toMetadata(withType: Boolean): Metadata = {
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

/** Factory methods for nominal attributes. */
object NominalAttribute extends AttributeFactory {

  /** The default nominal attribute. */
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
 * A binary attribute.
 * @param name optional name
 * @param index optional index
 * @param values optionla values. If set, its size must be 2.
 */
class BinaryAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    val values: Option[Array[String]] = None)
  extends Attribute {

  values.foreach { v =>
    require(v.length == 2, s"Number of values must be 2 for a binary attribute but got ${v.toSeq}.")
  }

  override def attrType: AttributeType = AttributeType.Binary

  override def isNumeric: Boolean = true

  override def isNominal: Boolean = true

  override def withName(name: String): BinaryAttribute = copy(name = Some(name))
  override def withoutName: BinaryAttribute = copy(name = None)

  override def withIndex(index: Int): BinaryAttribute = copy(index = Some(index))
  override def withoutIndex: BinaryAttribute = copy(index = None)

  /**
   * Copy with new values.
   * @param negative name for negative
   * @param positive name for positive
   */
  def withValues(negative: String, positive: String): BinaryAttribute =
    copy(values = Some(Array(negative, positive)))

  /** Copy without the values. */
  def withoutValues: BinaryAttribute = copy(values = None)

  /** Creates a copy of this attribute with optional changes. */
  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      values: Option[Array[String]] = values): BinaryAttribute = {
    new BinaryAttribute(name, index, values)
  }

  private[attribute] override def toMetadata(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val bldr = new MetadataBuilder
    if (withType) bldr.putString(TYPE, attrType.name)
    name.foreach(bldr.putString(NAME, _))
    index.foreach(bldr.putLong(INDEX, _))
    values.foreach(v => bldr.putStringArray(VALUES, v))
    bldr.build()
  }

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

  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + values.map(_.toSeq).hashCode
    sum
  }
}

/** Factory methods for binary attributes. */
object BinaryAttribute extends AttributeFactory {

  /** The default binary attribute. */
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
