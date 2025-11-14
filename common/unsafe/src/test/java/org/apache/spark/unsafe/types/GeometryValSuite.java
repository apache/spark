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

package org.apache.spark.unsafe.types;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class GeometryValSuite {

  @Test
  public void roundTripBytes() {
    // A simple byte array to test the round trip (`fromBytes` -> `getBytes`).
    byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
    GeometryVal geometryVal = GeometryVal.fromBytes(bytes);
    assertNotNull(geometryVal);
    assertArrayEquals(bytes, geometryVal.getBytes());
  }

  @Test
  public void roundNullHandling() {
    // A simple null byte array to test null handling for GEOMETRY.
    byte[] bytes = null;
    GeometryVal geometryVal = GeometryVal.fromBytes(bytes);
    assertNull(geometryVal);
  }

  @Test
  public void testCompareTo() {
    // Comparison is not yet supported for GEOMETRY.
    byte[] bytes1 = new byte[] { 1, 2, 3 };
    byte[] bytes2 = new byte[] { 4, 5, 6 };
    GeometryVal geometryVal1 = GeometryVal.fromBytes(bytes1);
    GeometryVal geometryVal2 = GeometryVal.fromBytes(bytes2);
    try {
      geometryVal1.compareTo(geometryVal2);
    } catch (UnsupportedOperationException e) {
      assert(e.toString().equals("java.lang.UnsupportedOperationException"));
    }
  }
}
