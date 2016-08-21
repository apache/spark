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

class AttributeGroupSuite extends SparkFunSuite {

  test("attribute group") {
    val attrs = Array(
      NumericAttribute.defaultAttr,
      NominalAttribute.defaultAttr,
      BinaryAttribute.defaultAttr.withIndex(0),
      NumericAttribute.defaultAttr.withName("age").withSparsity(0.8),
      NominalAttribute.defaultAttr.withName("size").withValues("small", "medium", "large"),
      BinaryAttribute.defaultAttr.withName("clicked").withValues("no", "yes"),
      NumericAttribute.defaultAttr,
      NumericAttribute.defaultAttr)
    val group = new AttributeGroup("user", attrs)
    assert(group.size === 8)
    assert(group.name === "user")
    assert(group(0) === NumericAttribute.defaultAttr.withIndex(0))
    assert(group(2) === BinaryAttribute.defaultAttr.withIndex(2))
    assert(group.indexOf("age") === 3)
    assert(group.indexOf("size") === 4)
    assert(group.indexOf("clicked") === 5)
    assert(!group.hasAttr("abc"))
    intercept[NoSuchElementException] {
      group("abc")
    }
    assert(group === AttributeGroup.fromMetadata(group.toMetadataImpl, group.name))
    assert(group === AttributeGroup.fromStructField(group.toStructField()))
  }

  test("attribute group without attributes") {
    val group0 = new AttributeGroup("user", 10)
    assert(group0.name === "user")
    assert(group0.numAttributes === Some(10))
    assert(group0.size === 10)
    assert(group0.attributes.isEmpty)
    assert(group0 === AttributeGroup.fromMetadata(group0.toMetadataImpl, group0.name))
    assert(group0 === AttributeGroup.fromStructField(group0.toStructField()))

    val group1 = new AttributeGroup("item")
    assert(group1.name === "item")
    assert(group1.numAttributes.isEmpty)
    assert(group1.attributes.isEmpty)
    assert(group1.size === -1)
  }
}
