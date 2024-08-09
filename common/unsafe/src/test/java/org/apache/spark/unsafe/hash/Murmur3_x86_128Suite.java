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

package org.apache.spark.unsafe.hash;

import org.apache.spark.unsafe.Platform;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test file largely based on Guava's Murmur3Hash128Test.
 */
public class Murmur3_x86_128Suite {

  private static final Murmur3_x86_128 hasher = new Murmur3_x86_128(0);

  @Test
  public void testKnownValues() {
    assertHash(0, "629942693e10f867", "92db0b82baeb5347", "hell");
    assertHash(1, "a78ddff5adae8d10", "128900ef20900135", "hello");
    assertHash(2, "8a486b23f422e826", "f962a2c58947765f", "hello ");
    assertHash(3, "2ea59f466f6bed8c", "c610990acc428a17", "hello w");
    assertHash(4, "79f6305a386c572c", "46305aed3483b94e", "hello wo");
    assertHash(5, "c2219d213ec1f1b5", "a1d8e2e0a52785bd", "hello wor");
    assertHash(0, "e34bbc7bbc071b6c", "7a433ca9c49a9347",
      "The quick brown fox jumps over the lazy dog");
    assertHash(0, "658ca970ff85269a", "43fee3eaa68e5c3e",
      "The quick brown fox jumps over the lazy cog");
  }

  private void assertHash(int seed, String expectedH1, String expectedH2, String stringInput) {
    byte[] in = ascii(stringInput);
    Murmur3_x86_128.HashObject hash = Murmur3_x86_128.hashUnsafeBytes(
      in, Platform.BYTE_ARRAY_OFFSET, in.length, seed);
    Assertions.assertEquals(expectedH1, Long.toHexString(hash.getHash1()));
    Assertions.assertEquals(expectedH2, Long.toHexString(hash.getHash2()));
  }

  private byte[] ascii(String string) {
    byte[] bytes = new byte[string.length()];
    for (int i = 0; i < string.length(); i++) {
      bytes[i] = (byte) string.charAt(i);
    }
    return bytes;
  }
}
