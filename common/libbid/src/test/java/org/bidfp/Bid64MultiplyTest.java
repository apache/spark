/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors may
 *     be used to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors may
 *     be used to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.bidfp;

/** Special-value and boundary tests for {@link Bid64Multiply}. */
public final class Bid64MultiplyTest {
  private Bid64MultiplyTest() {
  }

  public static void main(String[] args) {
    testSpecialValues();
    testExactFiniteValues();
    testFlagsAccumulate();
    System.out.println("Bid64MultiplyTest: all tests passed");
  }

  private static void testSpecialValues() {
    check(
        0x7c00_0000_0000_0000L,
        Bid64.POSITIVE_ZERO,
        Bid64.POSITIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
    check(
        0xf800_0000_0000_0000L,
        Bid64.finite(true, 398, 1L),
        Bid64.POSITIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        0);
    check(
        0x7c00_0000_0000_0123L,
        Bid64.fromRawBits(0x7e00_0000_0000_0123L),
        Bid64.finite(false, 398, 2L),
        RoundingMode.TIES_TO_EVEN,
        StatusFlags.INVALID);
  }

  private static void testExactFiniteValues() {
    check(
        Bid64.finite(false, 398, 42L).toRawBits(),
        Bid64.finite(false, 398, 6L),
        Bid64.finite(false, 398, 7L),
        RoundingMode.TIES_TO_EVEN,
        0);
    check(
        Bid64.finite(true, 0, 1_000_000_000_000_000L).toRawBits(),
        Bid64.finite(true, 0, 1_000_000_000_000_000L),
        Bid64.finite(false, 398, 1L),
        RoundingMode.TIES_TO_EVEN,
        0);
  }

  private static void testFlagsAccumulate() {
    StatusFlags flags = new StatusFlags();
    flags.raise(StatusFlags.DIVIDE_BY_ZERO);
    Bid64Multiply.multiply(
        Bid64.POSITIVE_ZERO,
        Bid64.POSITIVE_INFINITY,
        RoundingMode.TIES_TO_EVEN,
        flags);
    int expected = StatusFlags.DIVIDE_BY_ZERO | StatusFlags.INVALID;
    if (flags.bits() != expected) {
      throw new AssertionError(
          String.format("accumulated flags: expected %02x, actual %02x", expected, flags.bits()));
    }
  }

  private static void check(
      long expected,
      Bid64 x,
      Bid64 y,
      RoundingMode mode,
      int expectedFlags) {
    StatusFlags flags = new StatusFlags();
    long actual = Bid64Multiply.multiply(x, y, mode, flags).toRawBits();
    if (actual != expected || flags.bits() != expectedFlags) {
      throw new AssertionError(String.format(
          "multiply(%s, %s, %s): expected [0x%016x] %02x, actual [0x%016x] %02x",
          x, y, mode, expected, expectedFlags, actual, flags.bits()));
    }
  }
}
