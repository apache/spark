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

package org.apache.spark.ml.attribute;

import org.junit.Assert;
import org.junit.Test;

public class JavaAttributeSuite {

  @Test
  public void testAttributeType() {
    AttributeType numericType = AttributeType.Numeric();
    AttributeType nominalType = AttributeType.Nominal();
    AttributeType binaryType = AttributeType.Binary();
    Assert.assertEquals(numericType, NumericAttribute.defaultAttr().attrType());
    Assert.assertEquals(nominalType, NominalAttribute.defaultAttr().attrType());
    Assert.assertEquals(binaryType, BinaryAttribute.defaultAttr().attrType());
  }

  @Test
  public void testNumericAttribute() {
    NumericAttribute attr = NumericAttribute.defaultAttr()
      .withName("age").withIndex(0).withMin(0.0).withMax(1.0).withStd(0.5).withSparsity(0.4);
    Assert.assertEquals(attr.withoutIndex(), Attribute.fromStructField(attr.toStructField()));
  }

  @Test
  public void testNominalAttribute() {
    NominalAttribute attr = NominalAttribute.defaultAttr()
      .withName("size").withIndex(1).withValues("small", "medium", "large");
    Assert.assertEquals(attr.withoutIndex(), Attribute.fromStructField(attr.toStructField()));
  }

  @Test
  public void testBinaryAttribute() {
    BinaryAttribute attr = BinaryAttribute.defaultAttr()
      .withName("clicked").withIndex(2).withValues("no", "yes");
    Assert.assertEquals(attr.withoutIndex(), Attribute.fromStructField(attr.toStructField()));
  }
}
