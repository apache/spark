/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class CaseInsensitiveStringMapSuite {
  @Test
  public void testPutAndGet() {
    CaseInsensitiveStringMap options = CaseInsensitiveStringMap.empty();
    options.put("kEy", "valUE");

    Assert.assertEquals("Should return correct value for lower-case key",
        "valUE", options.get("key"));
    Assert.assertEquals("Should return correct value for upper-case key",
        "valUE", options.get("KEY"));
  }

  @Test
  public void testKeySet() {
    CaseInsensitiveStringMap options = CaseInsensitiveStringMap.empty();
    options.put("kEy", "valUE");

    Set<String> expectedKeySet = new HashSet<>();
    expectedKeySet.add("key");

    Assert.assertEquals("Should return lower-case key set", expectedKeySet, options.keySet());
  }
}
