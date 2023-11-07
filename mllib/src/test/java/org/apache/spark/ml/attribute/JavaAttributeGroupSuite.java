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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JavaAttributeGroupSuite {

  @Test
  public void testAttributeGroup() {
    Attribute[] attrs = new Attribute[]{
      NumericAttribute.defaultAttr(),
      NominalAttribute.defaultAttr(),
      BinaryAttribute.defaultAttr().withIndex(0),
      NumericAttribute.defaultAttr().withName("age").withSparsity(0.8),
      NominalAttribute.defaultAttr().withName("size").withValues("small", "medium", "large"),
      BinaryAttribute.defaultAttr().withName("clicked").withValues("no", "yes"),
      NumericAttribute.defaultAttr(),
      NumericAttribute.defaultAttr()
    };
    AttributeGroup group = new AttributeGroup("user", attrs);
    Assertions.assertEquals(8, group.size());
    Assertions.assertEquals("user", group.name());
    Assertions.assertEquals(NumericAttribute.defaultAttr().withIndex(0), group.getAttr(0));
    Assertions.assertEquals(3, group.indexOf("age"));
    Assertions.assertFalse(group.hasAttr("abc"));
    Assertions.assertEquals(group, AttributeGroup.fromStructField(group.toStructField()));
  }
}
