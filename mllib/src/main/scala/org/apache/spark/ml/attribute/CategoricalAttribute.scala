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

import org.apache.spark.sql.types.Metadata

class CategoricalAttribute private (
    override val index: Int,
    override val name: Option[String],
    override val dimension: Int,
    val categories: Option[Array[String]],
    val cardinality: Option[Int]) extends Attribute(index, name, dimension) {

  require(!categories.isDefined || categories.get.nonEmpty)
  require(!cardinality.isDefined || cardinality.get > 0)

  override def featureType: FeatureType = Categorical

  override def toMetadata(): Metadata = {
    val builder = toBaseMetadata()
    if (categories.isDefined) {
      builder.putStringArray("categories", categories.get)
    }
    if (cardinality.isDefined) {
      builder.putLong("cardinality", cardinality.get)
    }
    builder.build()
  }

}

private[attribute] object CategoricalAttribute {

  def fromMetadata(metadata: Metadata): CategoricalAttribute = {
    val (index, name, dimension) = Attribute.parseBaseMetadata(metadata)

    var cardinality: Option[Int] =
      if (metadata.contains("cardinality")) {
        Some(metadata.getLong("cardinality").toInt)
      } else {
        None
      }

    val categories: Option[Array[String]] =
      if (metadata.contains("categories")) {
        val theCategories = Some(metadata.getStringArray("categories"))
        if (cardinality.isDefined) {
          require(theCategories.get.size <= cardinality.get)
        } else {
          cardinality = Some(theCategories.get.size)
        }
        theCategories
      } else {
        None
      }

    new CategoricalAttribute(index, name, dimension, categories, cardinality)
  }

}