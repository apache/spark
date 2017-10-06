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

  test("VectorAttr") {
    val attr1 = NominalAttr(name = Some("col1Attr"), indicesRange = Seq(0))
    val attr2 = NumericAttr(name = Some("col2Attr"), indicesRange = Seq(1, 5))
    val vectAttr1 = VectorAttr(name = Some("vecAttr1"), attributes = Seq(attr1, attr2))
    assert(vectAttr1.getAttribute(0) === attr1.withoutName)
    assert(vectAttr1.getAttribute(1) === attr2.withoutName)

    val attr3 = NumericAttr(name = Some("col3Attr"), indicesRange = Seq(2, 5))
    val vectAttr2 = VectorAttr(name = Some("vecAttr2"), attributes = Seq(attr1, attr3))
    assert(vectAttr2.getAttribute(0) === attr1.withoutName)
    assert(vectAttr2.getAttribute(1) === UnresolvedMLAttribute)
    assert(vectAttr2.getAttribute(2) === attr3.withoutName)
  }
}

