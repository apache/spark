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
package org.bidfp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class Bid64ExpStatusTest {
  private static final long EXP_ZERO = 0x31c0_0000_0000_0000L;
  private static final int PREEXISTING_FLAGS = StatusFlags.INVALID | StatusFlags.INEXACT;

  @Test
  void infinityPathsPreservePreexistingFlags() {
    checkStickyFlags(Bid64.POSITIVE_INFINITY.toRawBits(), Bid64.MASK_INFINITY);
    checkStickyFlags(Bid64.NEGATIVE_INFINITY.toRawBits(), EXP_ZERO);
  }

  private static void checkStickyFlags(long input, long expected) {
    StatusFlags flags = flagsWithPreexistingBits();
    assertEquals(expected, Bid64Raw.exp(input, RoundingMode.TIES_TO_EVEN, flags));
    assertEquals(PREEXISTING_FLAGS, flags.bits());

    flags = flagsWithPreexistingBits();
    assertEquals(expected, Bid64Raw.exp2(input, RoundingMode.TIES_TO_EVEN, flags));
    assertEquals(PREEXISTING_FLAGS, flags.bits());

    flags = flagsWithPreexistingBits();
    assertEquals(expected, Bid64Raw.exp10(input, RoundingMode.TIES_TO_EVEN, flags));
    assertEquals(PREEXISTING_FLAGS, flags.bits());
  }

  private static StatusFlags flagsWithPreexistingBits() {
    StatusFlags flags = new StatusFlags();
    flags.raise(PREEXISTING_FLAGS);
    return flags;
  }
}
