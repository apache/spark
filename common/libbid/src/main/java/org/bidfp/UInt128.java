/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
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
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.bidfp;

/**
 * Unsigned 128-bit limb operations used by the BID64 and BID128 kernels.
 *
 * <p>Values wrap modulo 2^128. No {@code BigInteger} is used on the arithmetic
 * path.
 */
final class UInt128 implements Comparable<UInt128> {
  static final UInt128 ZERO = new UInt128(0L, 0L);

  private final long high;
  private final long low;

  UInt128(long high, long low) {
    this.high = high;
    this.low = low;
  }

  static UInt128 fromLong(long value) {
    return new UInt128(0L, value);
  }

  static UInt128 multiply(long x, long y) {
    return new UInt128(unsignedMultiplyHigh(x, y), x * y);
  }

  long high() {
    return high;
  }

  long low() {
    return low;
  }

  boolean isZero() {
    return (high | low) == 0L;
  }

  UInt128 multiply(long value) {
    return multiply(fromLong(value));
  }

  UInt128 add(UInt128 other) {
    long resultLow = low + other.low;
    long carry = Long.compareUnsigned(resultLow, low) < 0 ? 1L : 0L;
    return new UInt128(high + other.high + carry, resultLow);
  }

  UInt128 add(long value) {
    long resultLow = low + value;
    long carry = Long.compareUnsigned(resultLow, low) < 0 ? 1L : 0L;
    return new UInt128(high + carry, resultLow);
  }

  UInt128 subtract(UInt128 other) {
    long borrow = Long.compareUnsigned(low, other.low) < 0 ? 1L : 0L;
    return new UInt128(high - other.high - borrow, low - other.low);
  }

  UInt128 subtract(long value) {
    long borrow = Long.compareUnsigned(low, value) < 0 ? 1L : 0L;
    return new UInt128(high - borrow, low - value);
  }

  UInt128 multiply(UInt128 other) {
    long resultHigh = unsignedMultiplyHigh(low, other.low)
        + low * other.high
        + high * other.low;
    return new UInt128(resultHigh, low * other.low);
  }

  UInt128 shiftLeft(int distance) {
    if (distance < 0) {
      throw new IllegalArgumentException("negative shift distance");
    }
    if (distance == 0) {
      return this;
    }
    if (distance < 64) {
      return new UInt128(
          (high << distance) | (low >>> (64 - distance)),
          low << distance);
    }
    if (distance < 128) {
      return new UInt128(low << (distance - 64), 0L);
    }
    return ZERO;
  }

  UInt128 shiftRight(int distance) {
    if (distance < 0) {
      throw new IllegalArgumentException("negative shift distance");
    }
    if (distance == 0) {
      return this;
    }
    if (distance < 64) {
      return new UInt128(
          high >>> distance,
          (low >>> distance) | (high << (64 - distance)));
    }
    if (distance < 128) {
      return new UInt128(0L, high >>> (distance - 64));
    }
    return ZERO;
  }

  Division divide(long divisor) {
    if (divisor <= 0) {
      throw new IllegalArgumentException("divisor must be positive");
    }
    if (divisor <= 0xffff_ffffL) {
      return divideSmall(divisor);
    }
    long quotientHigh = 0;
    long quotientLow = 0;
    long remainder = 0;
    for (int bit = 127; bit >= 0; bit--) {
      boolean overflow = remainder < 0L;
      remainder = (remainder << 1) | bit(bit);
      if (overflow || Long.compareUnsigned(remainder, divisor) >= 0) {
        remainder -= divisor;
        if (bit >= 64) {
          quotientHigh |= 1L << (bit - 64);
        } else {
          quotientLow |= 1L << bit;
        }
      }
    }
    return new Division(new UInt128(quotientHigh, quotientLow), remainder);
  }

  private Division divideSmall(long divisor) {
    long quotientHigh = 0;
    long quotientLow = 0;
    long remainder = 0;
    for (int limb = 3; limb >= 0; limb--) {
      long digit = limb >= 2
          ? (high >>> ((limb - 2) * 32)) & 0xffff_ffffL
          : (low >>> (limb * 32)) & 0xffff_ffffL;
      long dividend = (remainder << 32) | digit;
      long quotientDigit = Long.divideUnsigned(dividend, divisor);
      remainder = Long.remainderUnsigned(dividend, divisor);
      if (limb >= 2) {
        quotientHigh |= quotientDigit << ((limb - 2) * 32);
      } else {
        quotientLow |= quotientDigit << (limb * 32);
      }
    }
    return new Division(new UInt128(quotientHigh, quotientLow), remainder);
  }

  String toDecimalString() {
    if (equals(ZERO)) {
      return "0";
    }
    char[] buffer = new char[39];
    int position = buffer.length;
    UInt128 value = this;
    while (!value.equals(ZERO)) {
      Division division = value.divide(10);
      buffer[--position] = (char) ('0' + division.remainder);
      value = division.quotient;
    }
    return new String(buffer, position, buffer.length - position);
  }

  private long bit(int index) {
    return index >= 64
        ? (high >>> (index - 64)) & 1L
        : (low >>> index) & 1L;
  }

  static long unsignedMultiplyHigh(long x, long y) {
    long result = Math.multiplyHigh(x, y);
    if (x < 0) {
      result += y;
    }
    if (y < 0) {
      result += x;
    }
    return result;
  }

  static final class Division {
    private final UInt128 quotient;
    private final long remainder;

    private Division(UInt128 quotient, long remainder) {
      this.quotient = quotient;
      this.remainder = remainder;
    }

    UInt128 quotient() {
      return quotient;
    }

    long remainder() {
      return remainder;
    }
  }

  @Override
  public int compareTo(UInt128 other) {
    int highComparison = Long.compareUnsigned(high, other.high);
    return highComparison != 0 ? highComparison : Long.compareUnsigned(low, other.low);
  }

  @Override
  public boolean equals(Object other) {
    return this == other
        || other instanceof UInt128
        && high == ((UInt128) other).high
        && low == ((UInt128) other).low;
  }

  @Override
  public int hashCode() {
    return 31 * Long.hashCode(high) + Long.hashCode(low);
  }

  @Override
  public String toString() {
    return String.format("0x%016x%016x", high, low);
  }
}
