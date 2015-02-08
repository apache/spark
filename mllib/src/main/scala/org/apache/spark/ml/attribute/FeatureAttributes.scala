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
 * Wrapper around [[Metadata]] with specialized methods for accessing information about
 * data as machine learning features, and their associated attributes, like:
 *
 * - type (continuous, categorical, etc.) as [[FeatureType]]
 * - for categorical features, the category values
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
    attributes.filter(_.featureType == FeatureType.CATEGORICAL).map(_.index)

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