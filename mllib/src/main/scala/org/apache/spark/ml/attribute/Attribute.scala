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

abstract class Attribute(val index: Int,
                         val name: Option[String],
                         val dimension: Int) {

  require(index >= 0)
  require(dimension >= 1)

  def featureType: FeatureType

  def toMetadata(): Metadata

  private[attribute] def toBaseMetadata(): MetadataBuilder = {
    val builder = new MetadataBuilder()
    builder.putLong("index", index)
    if (name.isDefined) {
      builder.putString("name", name.get)
    }
    if (dimension > 1) {
      builder.putLong("dimension", dimension)
    }
    builder
  }

}

object Attribute {

  def fromMetadata(metadata: Metadata): Attribute = {
    FeatureTypes.withName(metadata.getString("type")) match {
      case Categorical => CategoricalAttribute.fromMetadata(metadata)
      case Continuous => ContinuousAttribute.fromMetadata(metadata)
    }
  }

  private[attribute] def parseBaseMetadata(metadata: Metadata): (Int, Option[String], Int) = {
    val index = metadata.getLong("index").toInt
    val name = if (metadata.contains("name")) Some(metadata.getString("name")) else None
    val dimension = if (metadata.contains("dimension")) metadata.getLong("dimension").toInt else 1
    (index, name, dimension)
  }

}