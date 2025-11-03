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

public class GeographyValSuite {

  @Test
  public void roundTripBytes() {
    // A simple byte array to test the round trip (`fromBytes` -> `getBytes`).
    byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
    GeographyVal geographyVal = GeographyVal.fromBytes(bytes);
    assertNotNull(geographyVal);
    assertArrayEquals(bytes, geographyVal.getBytes());
  }

  @Test
  public void roundNullHandling() {
    // A simple null byte array to test null handling for GEOGRAPHY.
    byte[] bytes = null;
    GeographyVal geographyVal = GeographyVal.fromBytes(bytes);
    assertNull(geographyVal);
  }

  @Test
  public void testCompareTo() {
    // Comparison is not yet supported for GEOGRAPHY.
    byte[] bytes1 = new byte[] { 1, 2, 3 };
    byte[] bytes2 = new byte[] { 4, 5, 6 };
    GeographyVal geographyVal1 = GeographyVal.fromBytes(bytes1);
    GeographyVal geographyVal2 = GeographyVal.fromBytes(bytes2);
    try {
      geographyVal1.compareTo(geographyVal2);
    } catch (UnsupportedOperationException e) {
      assert(e.toString().equals("java.lang.UnsupportedOperationException"));
    }
  }
}
