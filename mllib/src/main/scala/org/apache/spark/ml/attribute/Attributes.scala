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
    values: Option[Array[String]] = None) extends SimpleAttribute with MetadataAPI {

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
    values: Option[Array[String]] = None) extends SimpleAttribute with MetadataAPI {

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
    sparsity: Option[Double] = None) extends SimpleAttribute with MetadataAPI {

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
    attributes: Seq[Attribute] = Seq.empty,
    numOfAttributes: Int = 0) extends ComplexAttribute with MetadataAPI {

  override val attrType: AttributeType = AttributeType.Complex

  override def withName(name: String): ComplexAttribute = copy(name = Some(name))
  override def withoutName: ComplexAttribute = copy(name = None)

  override def withAttributes(attributes: Seq[Attribute]): ComplexAttribute =
    copy(attributes = attributes)
  override def withoutAttributes: ComplexAttribute = copy(attributes = Seq.empty)

  override def withNumOfAttributes(num: Int): ComplexAttribute = copy(numOfAttributes = num)

  override def toMetadataImpl(): Metadata = {
    val bldr = new MetadataBuilder()

    bldr.putString(AttributeKeys.TYPE, attrType.name)
    name.foreach(bldr.putString(AttributeKeys.NAME, _))

    bldr.build()
  }
}

