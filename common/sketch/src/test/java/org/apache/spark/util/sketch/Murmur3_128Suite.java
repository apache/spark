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

package org.apache.spark.util.sketch;

import org.junit.Assert;
import org.junit.Test;

/**
 * Testing Murmur3 128 bit hasher.
 * All tests are taken from [[com.google.common.hash.Murmur3Hash128Test]]
 */
public class Murmur3_128Suite {

  @Test
  public void testKnownValues() throws Exception {
    assertHash(0, 0x629942693e10f867L, 0x92db0b82baeb5347L, "hell");
    assertHash(1, 0xa78ddff5adae8d10L, 0x128900ef20900135L, "hello");
    assertHash(2, 0x8a486b23f422e826L, 0xf962a2c58947765fL, "hello ");
    assertHash(3, 0x2ea59f466f6bed8cL, 0xc610990acc428a17L, "hello w");
    assertHash(4, 0x79f6305a386c572cL, 0x46305aed3483b94eL, "hello wo");
    assertHash(5, 0xc2219d213ec1f1b5L, 0xa1d8e2e0a52785bdL, "hello wor");
    assertHash(0, 0xe34bbc7bbc071b6cL, 0x7a433ca9c49a9347L,
            "The quick brown fox jumps over the lazy dog");
    assertHash(0, 0x658ca970ff85269aL, 0x43fee3eaa68e5c3eL,
            "The quick brown fox jumps over the lazy cog");
  }

  private static void assertHash(int seed, long expected1, long expected2, String stringInput) throws Exception {
    long[] hash128bit = new long[2];
    byte[] data = stringInput.getBytes("UTF-8");
    Murmur3_128.hashBytes(data, seed, hash128bit);
    Assert.assertEquals(expected1, hash128bit[0]);
    Assert.assertEquals(expected2, hash128bit[1]);
  }
}
