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
import org.apache.spark.sql.types.{DoubleType, Metadata, MetadataBuilder, StructField}

/**
 * :: DeveloperApi ::
 * The APIs for converting ML Attributes to SQL `Metadata` and `StructField`.
 */
@DeveloperApi
trait MetadataAPI {

  val name: Option[String]

  /**
   * Converts this attribute to `Metadata`.
   */
  def toMetadataImpl(): Metadata

  /** Converts to ML metadata with some existing metadata. */
  def toMetadata(existingMetadata: Metadata): Metadata = {
    new MetadataBuilder()
      .withMetadata(existingMetadata)
      .putMetadata(AttributeKeys.ML_ATTRV2, toMetadataImpl())
      .build()
  }

  /** Converts to ML metadata */
  def toMetadata(): Metadata = toMetadata(Metadata.empty)


  /**
   * Converts to a `StructField` with some existing metadata.
   * @param existingMetadata existing metadata to carry over
   */
  def toStructField(existingMetadata: Metadata): StructField = {
    require(name.isDefined, "Must define attribute name before converting to `StructField`.")
    val newMetadata = toMetadata(existingMetadata)
    StructField(name.get, DoubleType, nullable = false, newMetadata)
  }

  /**
   * Converts to a `StructField`.
   */

  def toStructField(): StructField = toStructField(Metadata.empty)
}

@DeveloperApi
object MLAttributes {
  /**
   * Creates an `Attribute` from a `Metadata` instance.
   */
  def fromMetadata(metadata: Metadata): BaseAttribute = {
    if (metadata.contains(AttributeKeys.TYPE)) {
      val attrType = metadata.getString(AttributeKeys.TYPE)
      getFactory(attrType).fromMetadata(metadata)
    } else {
      UnresolvedMLAttribute
    }
  }

  /** Gets the attribute factory given the attribute type name. */
  private def getFactory(attrType: String): MLAttributeFactory = {
    if (attrType == AttributeType.Numeric.name) {
      NumericAttrFactory
    } else if (attrType == AttributeType.Nominal.name) {
      NominalAttrFactory
    } else if (attrType == AttributeType.Binary.name) {
      BinaryAttrFactory
    } else if (attrType == AttributeType.Complex.name) {
      ComplexAttrFactory
    } else {
      throw new IllegalArgumentException(s"Cannot recognize type $attrType.")
    }
  }

  /**
   * Creates an `Attribute` from a `StructField` instance, optionally preserving name.
   */
  private[ml] def fromStructField(field: StructField, preserveName: Boolean): BaseAttribute = {
    val metadata = field.metadata
    val mlAttr = AttributeKeys.ML_ATTRV2
    if (metadata.contains(mlAttr)) {
      val attr = fromMetadata(metadata.getMetadata(mlAttr))
      if (preserveName) {
        attr
      } else {
        attr.withName(field.name)
      }
    } else {
      UnresolvedMLAttribute
    }
  }
}

/** Trait for ML attribute factories. */
@DeveloperApi
trait MLAttributeFactory {
  def fromMetadata(metadata: Metadata): BaseAttribute

  def loadCommonMetadata(metadata: Metadata): (Option[String], Seq[String], Seq[Int]) = {
    val name = if (metadata.contains(AttributeKeys.NAME)) {
      Some(metadata.getString(AttributeKeys.NAME))
    } else {
      None
    }

    val indices = if (metadata.contains(AttributeKeys.INDICES)) {
      metadata.getLongArray(AttributeKeys.INDICES).map(_.toInt).toSeq
    } else {
      Seq.empty
    }

    val names = if (metadata.contains(AttributeKeys.NAMES)) {
      metadata.getStringArray(AttributeKeys.NAMES).toSeq
    } else {
      Seq.empty
    }

    (name, names, indices)
  }
}

@DeveloperApi
object NumericAttrFactory extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): NumericAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, names, indices) = loadCommonMetadata(metadata)

    val min = if (metadata.contains(MIN)) Some(metadata.getDouble(MIN)) else None
    val max = if (metadata.contains(MAX)) Some(metadata.getDouble(MAX)) else None
    val std = if (metadata.contains(STD)) Some(metadata.getDouble(STD)) else None
    val sparsity = if (metadata.contains(SPARSITY)) Some(metadata.getDouble(SPARSITY)) else None

    NumericAttr(name, names, indices, min, max, std, sparsity)
  }
}

@DeveloperApi
object BinaryAttrFactory extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): BinaryAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, names, indices) = loadCommonMetadata(metadata)

    val values = if (metadata.contains(VALUES)) {
      Some(metadata.getStringArray(VALUES))
    } else {
      None
    }

    BinaryAttr(name, names, indices, values)
  }
}

@DeveloperApi
object NominalAttrFactory extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): NominalAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, names, indices) = loadCommonMetadata(metadata)

    val isOrdinal = if (metadata.contains(ORDINAL)) Some(metadata.getBoolean(ORDINAL)) else None
    val values = if (metadata.contains(VALUES)) {
      Some(metadata.getStringArray(VALUES))
    } else {
      None
    }

    NominalAttr(name, names, indices, isOrdinal, values)
  }
}

object ComplexAttrFactory extends MLAttributeFactory {
  override def fromMetadata(metadata: Metadata): ComplexAttr = {
    import org.apache.spark.ml.attribute.AttributeKeys._

    val (name, names, indices) = loadCommonMetadata(metadata)

    // NominalAttr(name, names, indices, isOrdinal, values)
    null
  }
}
