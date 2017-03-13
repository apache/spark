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

class ContinuousAttribute private (
    override val index: Int,
    override val name: Option[String],
    override val dimension: Int,
    val min: Option[Double],
    val max: Option[Double]) extends Attribute(index, name, dimension) {

  if (min.isDefined && max.isDefined) {
    require(min.get <= max.get)
  }

  override def featureType(): FeatureType = Continuous

  override def toMetadata(): Metadata = {
    val builder = toBaseMetadata()
    if (min.isDefined) {
      builder.putDouble("min", min.get)
    }
    if (max.isDefined) {
      builder.putDouble("max", max.get)
    }
    builder.build()
  }

}

private[attribute] object ContinuousAttribute {

  def fromMetadata(metadata: Metadata): ContinuousAttribute = {
    val (index, name, dimension) = Attribute.parseBaseMetadata(metadata)
    val min = if (metadata.contains("min")) Some(metadata.getDouble("min")) else None
    val max = if (metadata.contains("max")) Some(metadata.getDouble("max")) else None
    new ContinuousAttribute(index, name, dimension, min, max)
  }

}
