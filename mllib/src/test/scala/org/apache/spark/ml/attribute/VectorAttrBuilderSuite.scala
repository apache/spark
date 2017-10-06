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

  test("build VectorAttr") {
    val fields1 = Seq(
      StructField("col1", DoubleType),
      StructField("col2", DoubleType)
    )
    val attr1 = VectorAttrBuilder.buildAttr(fields1, Seq(0, 0))
    assert(attr1.attributes.length == 1 &&
      attr1.attributes(0).isInstanceOf[NumericAttr] &&
      attr1.attributes(0).indicesRange === Seq(0, 1))

    val col1Metadata = NominalAttr(name = Some("col1Attr"), indicesRange = Seq(5))
      .toStructField.metadata
    val fields2 = Seq(
      StructField("col1", DoubleType, metadata = col1Metadata),
      StructField("col2", DoubleType)
    )
    val attr2 = VectorAttrBuilder.buildAttr(fields2, Seq(0, 0))
    assert(attr2.attributes.length == 2 &&
      attr2.attributes(0).isInstanceOf[NominalAttr] &&
      attr2.attributes(0).indicesRange === Seq(0) &&
      attr2.attributes(1).isInstanceOf[NumericAttr] &&
      attr2.attributes(1).indicesRange === Seq(1))

    // 0 to 5 dimensions in this vector attribute are nominal attributes.
    // 6 dimension in this vector attribute are numeric attribute.
    val col2Metadata = VectorAttr(
      name = Some("col2Attr"),
      attributes = Seq(
        NominalAttr(indicesRange = Seq(0, 5)),
        NumericAttr(indicesRange = Seq(6))
      )
    ).toStructField.metadata
    val fields3 = Seq(
      StructField("col1", DoubleType),
      StructField("col2", new VectorUDT(), metadata = col2Metadata),
      StructField("col3", DoubleType)
    )
    val attr3 = VectorAttrBuilder.buildAttr(fields3, Seq(0, 2, 0))
    assert(attr3.attributes.length == 4 &&
      attr3.attributes(0).isInstanceOf[NumericAttr] &&
      attr3.attributes(0).indicesRange === Seq(0) &&
      attr3.attributes(1).isInstanceOf[NominalAttr] &&
      attr3.attributes(1).indicesRange === Seq(1, 6) &&
      attr3.attributes(2).isInstanceOf[NumericAttr] &&
      attr3.attributes(2).indicesRange === Seq(7) &&
      attr3.attributes(3).isInstanceOf[NumericAttr] &&
      attr3.attributes(3).indicesRange === Seq(8))
  }
}
