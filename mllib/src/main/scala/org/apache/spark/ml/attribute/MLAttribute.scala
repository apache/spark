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

/**
 * :: DeveloperApi ::
 * The base abstraction for ML attributes that describe ML columns.
 * A ML attribute has an attribute type and an optinal name.
 */
@DeveloperApi
sealed trait MLAttribute {

  /** Attribute type. */
  val attrType: AttributeType

  /** Optional name of the attribute. */
  val name: Option[String]
}

/**
 * :: DeveloperApi ::
 * Describes ML inner attributes which are included in a ML vector column.
 */
@DeveloperApi
sealed trait InnerAttribute {

  /**
   * The range of indices of the attribute:
   * An `InnerAttribute` can represent multiple ML columns if those columns share the same
   * properties. This range can be at most 2 indices which represent the beginning and ending
   * indices. It can be empty so that this attribute isn't included in a vector column.
   */
  val indicesRange: Seq[Int]

  require(indicesRange.length <= 2, "Range of indices should be less than or equal to 2.")
  require(indicesRange == indicesRange.sorted, "Range of indices must be in ascending order.")

  def getMinIndex(): Int = indicesRange.min
  def getMaxIndex(): Int = indicesRange.max
}

/**
 * :: DeveloperApi ::
 * The basic operations of ML attributes.
 */
@DeveloperApi
abstract class BaseAttribute extends MLAttribute with Serializable {
  def withName(name: String): BaseAttribute
  def withoutName(): BaseAttribute
}

/**
 * :: DeveloperApi ::
 * The basic operations of ML simple attributes which can't include other attributes.
 */
@DeveloperApi
abstract class SimpleAttribute
    extends BaseAttribute with InnerAttribute with MetadataInterface {

  def withName(name: String): SimpleAttribute
  def withoutName(): SimpleAttribute

  def withIndicesRange(indices: Seq[Int]): SimpleAttribute
  def withoutIndicesRange: SimpleAttribute

  def addIntoComplexAttribute(index: Int, complexAttr: ComplexAttribute): Unit = {
    complexAttr.addAttribute(withIndicesRange(Seq(index)))
  }
  def addIntoComplexAttribute(beginIdx: Int, endIdx: Int, complexAttr: ComplexAttribute): Unit = {
    complexAttr.addAttribute(withIndicesRange(Seq(beginIdx, endIdx)))
  }
}

/**
 * :: DeveloperApi ::
 * The basic operations of ML complex attributes which can include other attributes.
 */
@DeveloperApi
abstract class ComplexAttribute
    extends BaseAttribute with MetadataInterface {

  def numOfAttributes: Int
  def attributes: Seq[SimpleAttribute]

  def withName(name: String): ComplexAttribute
  def withoutName(): ComplexAttribute

  // Add an inner attribute into this complex attribute. Note the addition checks if added
  // attribute has indices following previous added attributes.
  def addAttribute(attr: SimpleAttribute): this.type
  def addAttributes(attrs: Seq[SimpleAttribute]): this.type = {
    attrs.map(addAttribute)
    this
  }

  def getAttribute(idx: Int): BaseAttribute
}

@DeveloperApi
case object UnresolvedMLAttribute extends BaseAttribute with Serializable {
  val attrType: AttributeType = AttributeType.Unresolved
  val name: Option[String] = None

  def withName(name: String): BaseAttribute = this
  def withoutName(): BaseAttribute = this
}
