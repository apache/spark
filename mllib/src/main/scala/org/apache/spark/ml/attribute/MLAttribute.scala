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
 * The base abstraction for ML Attributes that describe ML columns.
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
 * Describes ML inner Attributes which are included in a ML vector column.
 */
@DeveloperApi
sealed trait InnerAttribute {

  /** Names of the attribute. */
  val names: Seq[String]

  /** Indices of the attribute. */
  val indices: Seq[Int]
}

/**
 * :: DeveloperApi ::
 * Describes ML Attributes that describe ML vector columns.
 */
@DeveloperApi
sealed trait VectorAttribute {

  /** Indices of the attribute. */
  val attributes: Seq[InnerAttribute]

  /** The number of attributes. */
  // val numOfAttributes: Int
}

/**
 * :: DeveloperApi ::
 * The basic operations of ML Attributes.
 */
@DeveloperApi
abstract class BaseAttribute extends MLAttribute with Serializable {
  def withName(name: String): BaseAttribute
  def withoutName: BaseAttribute
}

/**
 * :: DeveloperApi ::
 * The basic operations of ML Attributes.
 */
@DeveloperApi
abstract class SimpleAttribute extends BaseAttribute with InnerAttribute with MetadataAPI {
  def withIndices(indices: Seq[Int]): SimpleAttribute
  def withoutIndices: SimpleAttribute

  def withNames(names: Seq[String]): SimpleAttribute
  def withoutNames: SimpleAttribute
}

/**
 * :: DeveloperApi ::
 * The basic operations of ML vector Attributes.
 */
@DeveloperApi
abstract class ComplexAttribute extends BaseAttribute with VectorAttribute with MetadataAPI {
  def withAttributes(attributes: Seq[SimpleAttribute]): ComplexAttribute
  def withoutAttributes: ComplexAttribute

  // def withNumOfAttributes(num: Int): ComplexAttribute
}

case object UnresolvedMLAttribute extends BaseAttribute with Serializable {

  val attrType: AttributeType = AttributeType.Unresolved
  val name: Option[String] = None

  def withName(name: String): BaseAttribute = this
  def withoutName: BaseAttribute = this
}
