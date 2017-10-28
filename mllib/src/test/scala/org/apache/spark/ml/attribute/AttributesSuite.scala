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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class AttributesSuite extends SparkFunSuite {
  import AttributeKeys._
  import AttributeType._

  test("NominalAttr: default constructor") {
    val attr = NominalAttr()
    val metadata = attr.toMetadata().getMetadata(ML_ATTRV2)
    assert(metadata.getString(TYPE) == Nominal.name && !metadata.contains(NAME))
  }

  test("NumericAttr: default constructor") {
    val attr = NumericAttr()
    val metadata = attr.toMetadata().getMetadata(ML_ATTRV2)
    assert(metadata.getString(TYPE) == Numeric.name && !metadata.contains(NAME))
  }

  test("BinaryAttr: default constructor") {
    val attr = BinaryAttr()
    val metadata = attr.toMetadata().getMetadata(ML_ATTRV2)
    assert(metadata.getString(TYPE) == Binary.name && !metadata.contains(NAME))
  }

  test("VectorAttr: default constructor") {
    val attr = VectorAttr(1)
    val metadata = attr.toMetadata().getMetadata(ML_ATTRV2)
    assert(metadata.getString(TYPE) == Vector.name
      && !metadata.contains(NAME) &&
      metadata.getLong(NUM_ATTRIBUTES) == 1L)
  }

  test("NominalAttr") {
    val attr1 = NominalAttr(name = Some("col1"))
    val attr1Metadata = attr1.toMetadata().getMetadata(ML_ATTRV2)
    assert(attr1Metadata.getString(TYPE) == Nominal.name &&
      attr1Metadata.getString(NAME) == "col1")

    val structField1 = attr1.toStructField()
    assert(attr1 == MLAttributes.fromStructField(structField1, preserveName = true))

    val attr2 = NominalAttr(name = Some("col2"),
      isOrdinal = Some(true),
      values = Some(Array("small", "large")))
    val attr2Metadata = attr2.toMetadata().getMetadata(ML_ATTRV2)
    assert(attr2Metadata.getString(TYPE) == Nominal.name &&
      attr2Metadata.getString(NAME) == "col2" &&
      attr2Metadata.getBoolean(ORDINAL) == true &&
      attr2Metadata.getStringArray(VALUES) === Array("small", "large"))

    val structField2 = attr2.toStructField()
    assert(attr2 == MLAttributes.fromStructField(structField2, preserveName = true))

  }

  test("NumericAttr") {
    val attr1 = NumericAttr(name = Some("col1"))
    val attr1Metadata = attr1.toMetadata().getMetadata(ML_ATTRV2)
    assert(attr1Metadata.getString(TYPE) == Numeric.name &&
      attr1Metadata.getString(NAME) == "col1")

    val structField1 = attr1.toStructField()
    assert(attr1 == MLAttributes.fromStructField(structField1, preserveName = true))

    val attr2 = NumericAttr(name = Some("col2"),
      min = Some(1.0),
      max = Some(10.0),
      std = Some(2.5),
      sparsity = Some(0.5))
    val attr2Metadata = attr2.toMetadata().getMetadata(ML_ATTRV2)
    assert(attr2Metadata.getString(TYPE) == Numeric.name &&
      attr2Metadata.getString(NAME) == "col2" &&
      attr2Metadata.getDouble(MIN) == 1.0 &&
      attr2Metadata.getDouble(MAX) == 10.0 &&
      attr2Metadata.getDouble(STD) == 2.5 &&
      attr2Metadata.getDouble(SPARSITY) == 0.5)

    val structField2 = attr2.toStructField()
    assert(attr2 == MLAttributes.fromStructField(structField2, preserveName = true))
  }

  test("BinaryAttr") {
    val attr1 = BinaryAttr(name = Some("col1"))
    val attr1Metadata = attr1.toMetadata().getMetadata(ML_ATTRV2)
    assert(attr1Metadata.getString(TYPE) == Binary.name &&
      attr1Metadata.getString(NAME) === "col1")

    val structField1 = attr1.toStructField()
    assert(attr1 == MLAttributes.fromStructField(structField1, preserveName = true))

    val attr2 = BinaryAttr(name = Some("col2"),
      values = Some(Array("1.0", "2.0")))
    val attr2Metadata = attr2.toMetadata().getMetadata(ML_ATTRV2)
    assert(attr2Metadata.getString(TYPE) == Binary.name &&
      attr2Metadata.getStringArray(VALUES) === Array("1.0", "2.0"))

    val structField2 = attr2.toStructField()
    assert(attr2 == MLAttributes.fromStructField(structField2, preserveName = true))
  }

  test("VectorAttr: metadata") {
    val attr1 = NominalAttr(name = Some("col1Attr"), indicesRange = Seq(0))
    val attr2 = NumericAttr(name = Some("col2Attr"), indicesRange = Seq(1, 5))
    val vecAttr1 = VectorAttr(numOfAttributes = 6, name = Some("vecAttr1"))
      .addAttributes(Array(attr1, attr2))
    val attrArray = vecAttr1.toMetadata().getMetadata(ML_ATTRV2)
      .getMetadataArray(ATTRIBUTES)
    val metadataForAttr1 = attrArray(0).getMetadata(ML_ATTRV2)
    val metadataForAttr2 = attrArray(1).getMetadata(ML_ATTRV2)
    assert(metadataForAttr1.getString(TYPE) === Nominal.name &&
      metadataForAttr1.getLongArray(INDICES) === Array(0L))
    assert(metadataForAttr2.getString(TYPE) === Numeric.name &&
      metadataForAttr2.getLongArray(INDICES) === Array(1L, 5L))

    val structField = vecAttr1.toStructField()
    assert(vecAttr1 == MLAttributes.fromStructField(structField, preserveName = true))
  }

  test("VectorAttr: getAttribute") {
    val attr1 = NominalAttr(name = Some("col1Attr"), indicesRange = Seq(0))
    val attr2 = NumericAttr(name = Some("col2Attr"), indicesRange = Seq(1, 5))
    val vecAttr1 = VectorAttr(numOfAttributes = 6, name = Some("vecAttr1"))
      .addAttributes(Array(attr1, attr2))
    assert(vecAttr1.getAttribute(0) === attr1.withoutName)
    assert(vecAttr1.getAttribute(1) === attr2.withoutName)

    val attr3 = NumericAttr(name = Some("col3Attr"), indicesRange = Seq(2, 5))
    val vecAttr2 = VectorAttr(numOfAttributes = 6, name = Some("vecAttr2"))
      .addAttributes(Array(attr1, attr3))
    assert(vecAttr2.getAttribute(0) === attr1.withoutName)
    assert(vecAttr2.getAttribute(1) === UnresolvedMLAttribute)
    assert(vecAttr2.getAttribute(2) === attr3.withoutName)
  }
}

