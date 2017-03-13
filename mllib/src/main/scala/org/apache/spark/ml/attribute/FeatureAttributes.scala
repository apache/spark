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

import org.apache.spark.sql.types.{MetadataBuilder, Metadata}

/**
 * Representation of specialized information in a [[Metadata]] concerning
 * data as machine learning features, with methods to access their associated attributes, like:
 *
 * - type (continuous, categorical, etc.) as [[FeatureType]]
 * - optional feature name
 * - for categorical features, the category values
 * - for continuous values, maximum and minimum value
 * - dimension for vector-valued features
 *
 * This information is stored as a [[Metadata]] under key "features", and contains an array of
 * [[Metadata]] inside that for each feature for which metadata is defined. Example:
 *
 * {{{
 *   {
 *     ...
 *     "features" : [
 *       {
 *         "index": 0,
 *         "name": "age",
 *         "type": "CONTINUOUS",
 *         "min": 0
 *       },
 *       {
 *         "index": 5,
 *         "name": "gender",
 *         "type": "CATEGORICAL",
 *         "categories" : [ "male", "female" ]
 *       },
 *       {
 *         "index": 6,
 *         "name": "customerType",
 *         "type": "CATEGORICAL",
 *         "cardinality": 10
 *       },
 *       {
 *         "index": 7,
 *         "name": "percentAllocations",
 *         "type": "CONTINUOUS",
 *         "dimension": 10,
 *         "min": 0,
 *         "max": 1
 *     ]
 *     "producer": "..."
 *     ...
 *   }
 * }}}
 */
class FeatureAttributes private (val attributes: Array[Attribute],
                                 val producer: Option[String]) {

  private val nameToIndex: Map[String,Int] =
    attributes.filter(_.name.isDefined).map(att => (att.name.get, att.index)).toMap
  private val indexToAttribute: Map[Int,Attribute] =
    attributes.map(att => (att.index, att)).toMap
  private val categoricalIndices: Array[Int] =
    attributes.filter(_.featureType match {
      case c: CategoricalFeatureType  => true
      case _ => false
    }).map(_.index)

  def getFeatureAttribute(index: Int): Option[Attribute] = indexToAttribute.get(index)

  def getFeatureIndex(featureName: String): Option[Int] = nameToIndex.get(featureName)

  def categoricalFeatureIndices(): Array[Int] = categoricalIndices

  def toMetadata(): Metadata = {
    val builder = new MetadataBuilder()
    builder.putMetadataArray("features", attributes.map(_.toMetadata()))
    if (producer.isDefined) {
      builder.putString("producer", producer.get)
    }
    builder.build()
  }

}

object FeatureAttributes {

  def fromMetadata(metadata: Metadata): FeatureAttributes = {
    val attributes = metadata.getMetadataArray("features").map(Attribute.fromMetadata(_))
    val producer =
      if (metadata.contains("producer")) Some(metadata.getString("producer")) else None
    new FeatureAttributes(attributes, producer)
  }

}