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
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.types._

class VectorAttrBuilderSuite extends SparkFunSuite {

  test("build VectorAttr: simple attributes") {
    val fields1 = Array(
      StructField("col1", DoubleType),
      StructField("col2", DoubleType)
    )
    val attr1 = VectorAttrBuilder.buildAttr(fields1, attributeSizes = Array(1, 1))
    assert(attr1.numOfAttributes == 2 &&
      attr1.getAttribute(0).isInstanceOf[NumericAttr] &&
      attr1.getAttribute(0).asInstanceOf[NumericAttr].indicesRange === Seq(0, 1))
  }

  test("build VectorAttr: simple attribute with metadata") {
    val col1Metadata = NominalAttr(name = Some("col1Attr"))
      .toStructField.metadata
    val fields2 = Array(
      StructField("col1", DoubleType, metadata = col1Metadata),
      StructField("col2", DoubleType)
    )
    val attr2 = VectorAttrBuilder.buildAttr(fields2, attributeSizes = Array(1, 1))
    assert(attr2.numOfAttributes == 2 &&
      attr2.getAttribute(0).isInstanceOf[NominalAttr] &&
      attr2.getAttribute(0).asInstanceOf[NominalAttr].indicesRange === Seq(0) &&
      attr2.getAttribute(1).isInstanceOf[NumericAttr] &&
      attr2.getAttribute(1).asInstanceOf[NumericAttr].indicesRange === Seq(1))
  }

  test("build VectorAttr: nested vector attribute") {
    // 0 to 5 dimensions in this vector attribute are nominal attributes.
    // 6 dimension in this vector attribute are numeric attribute.
    // This vector has totally 7 attributes.
    val col2Metadata = VectorAttr(numOfAttributes = 7, name = Some("col2Attr"))
      .addAttributes(Array(
        NominalAttr(indicesRange = Seq(0, 5)),
        NumericAttr(indicesRange = Seq(6))
      )).toStructField.metadata

    val col4Metadata = NominalAttr(name = Some("col4Attr"))
      .toStructField.metadata

    val fields3 = Array(
      StructField("col1", DoubleType),
      StructField("col2", new VectorUDT(), metadata = col2Metadata),
      StructField("col3", DoubleType),
      StructField("col4", DoubleType, metadata = col4Metadata)
    )
    val attr3 = VectorAttrBuilder.buildAttr(fields3, attributeSizes = Array(1, 7, 1, 1))

    // There're 10 attributes in this vector.
    // [0] is a numeric attribute. [1-6] are nominal attributes.
    // [7-8] are numeric attributes. [9] is a nominal attribute.
    assert(attr3.numOfAttributes == 10 &&
      attr3.getAttribute(0).isInstanceOf[NumericAttr] &&
      attr3.getAttribute(0).asInstanceOf[NumericAttr].indicesRange === Seq(0) &&
      attr3.getAttribute(1).isInstanceOf[NominalAttr] &&
      attr3.getAttribute(1).asInstanceOf[NominalAttr].indicesRange === Seq(1, 6) &&
      attr3.getAttribute(7).isInstanceOf[NumericAttr] &&
      attr3.getAttribute(7).asInstanceOf[NumericAttr].indicesRange === Seq(7, 8) &&
      attr3.getAttribute(9).isInstanceOf[NominalAttr] &&
      attr3.getAttribute(9).asInstanceOf[NominalAttr].indicesRange === Seq(9))
  }

  test("sameAttrProps") {
    val attr1 = NominalAttr(name = Some("col1Attr"), indicesRange = Seq(0))
    val attr2 = NominalAttr(name = Some("col2Attr"), indicesRange = Seq(1, 5))
    val attr3 = NumericAttr(name = Some("col3Attr"), min = Some(1), indicesRange = Seq(1))
    val attr4 = NumericAttr(name = Some("col4Attr"), max = Some(10), indicesRange = Seq(1))
    val attr5 = NumericAttr(name = Some("col5Attr"), min = Some(1), indicesRange = Seq(2))
    assert(VectorAttrBuilder.sameAttrProps(attr1, attr2))
    assert(!VectorAttrBuilder.sameAttrProps(attr1, attr3))
    assert(!VectorAttrBuilder.sameAttrProps(attr3, attr4))
    assert(VectorAttrBuilder.sameAttrProps(attr3, attr5))
  }
}
