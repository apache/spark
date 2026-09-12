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
 * Mutable decimal integer {@code +/- limbs * 10^exp} using 9-digit limbs.
 * Used for conversion, quantize, FMA, and integer remainder; not the hot
 * add/mul/div kernels.
 */
final class DecNum {
  private static final int BASE = 1_000_000_000;
  private static final int MAX_LIMBS = 4096;

  private int[] limbs = new int[8];
  private int length;
  private boolean negative;
  private int exp;

  DecNum() {
    length = 1;
  }

  static DecNum ofLong(long value) {
    DecNum result = new DecNum();
    result.setLong(value);
    return result;
  }

  static DecNum ofUnsigned(long high, long low) {
    DecNum result = new DecNum();
    result.setUnsigned(high, low);
    return result;
  }

  static DecNum ofCoefficient(boolean negative, long coefficient, int unbiasedExp) {
    DecNum result = ofLong(coefficient);
    result.negative = negative;
    result.exp = unbiasedExp;
    return result;
  }

  boolean isZero() {
    return length == 1 && limbs[0] == 0;
  }

  boolean isNegative() {
    return negative;
  }

  void setNegative() {
    negative = true;
  }

  void setNegative(boolean value) {
    negative = value;
  }

  void shiftExp(int delta) {
    exp += delta;
  }

  void addDigit(int digit) {
    addAbsolute(DecNum.ofLong(digit));
  }

  int exp() {
    return exp;
  }

  void setLong(long value) {
    clear();
    if (value == 0L) {
      return;
    }
    negative = value < 0;
    long magnitude = value == Long.MIN_VALUE ? value : Math.abs(value);
    if (value == Long.MIN_VALUE) {
      // 9223372036854775808 = 9_223_372_036 * 10^9 + 854_775_808
      limbs[0] = 854_775_808;
      limbs[1] = 223_372_036;
      limbs[2] = 9;
      length = 3;
      return;
    }
    long remaining = magnitude;
    int i = 0;
    while (remaining != 0L) {
      limbs[i++] = (int) (remaining % BASE);
      remaining /= BASE;
    }
    length = i;
  }

  void setUnsigned(long high, long low) {
    clear();
    if ((high | low) == 0L) {
      return;
    }
    DecNum acc = ofLong(0L);
    if (high != 0L) {
      acc.setUnsigned64(high);
      acc.shiftLeftBits(64);
    }
    DecNum lowPart = new DecNum();
    lowPart.setUnsigned64(low);
    acc.addAbsolute(lowPart);
    copyFrom(acc);
  }

  private void setUnsigned64(long value) {
    clear();
    if (value == 0L) {
      return;
    }
    long remaining = value;
    int i = 0;
    for (; remaining != 0L && i < MAX_LIMBS; i++) {
      long q = Long.divideUnsigned(remaining, BASE);
      limbs[i] = (int) (remaining - q * BASE);
      remaining = q;
    }
    length = i;
  }

  void clear() {
    for (int i = 0; i < length; i++) {
      limbs[i] = 0;
    }
    length = 1;
    negative = false;
    exp = 0;
  }

  void copyFrom(DecNum other) {
    clear();
    negative = other.negative;
    exp = other.exp;
    ensureRoom(other.length);
    length = other.length;
    System.arraycopy(other.limbs, 0, limbs, 0, length);
  }

  int digitCount() {
    if (isZero()) {
      return 1;
    }
    int digits = (length - 1) * 9;
    int top = limbs[length - 1];
    int extra = 1;
    int p = 10;
    while (p <= top) {
      extra++;
      if (p > Integer.MAX_VALUE / 10) {
        break;
      }
      p *= 10;
    }
    if (p <= top) {
      extra = PowersOfTen.decimalDigits(top);
    }
    return digits + extra;
  }

  void multiplyBy10() {
    multiplySmall(10);
  }

  void multiplySmall(int factor) {
    if (factor == 0) {
      clear();
      return;
    }
    if (factor == 1 || isZero()) {
      return;
    }
    long carry = 0;
    for (int i = 0; i < length; i++) {
      long product = (long) limbs[i] * factor + carry;
      limbs[i] = (int) (product % BASE);
      carry = product / BASE;
    }
    while (carry != 0) {
      ensureRoom();
      limbs[length++] = (int) (carry % BASE);
      carry /= BASE;
    }
  }

  void multiplyPow10(int n) {
    if (n <= 0 || isZero()) {
      if (n < 0) {
        throw new IllegalArgumentException("use dividePow10");
      }
      return;
    }
    int nines = n / 9;
    int rest = n % 9;
    if (rest != 0) {
      multiplySmall(pow10int(rest));
    }
    if (nines != 0) {
      ensureRoom(length + nines);
      System.arraycopy(limbs, 0, limbs, nines, length);
      for (int i = 0; i < nines; i++) {
        limbs[i] = 0;
      }
      length += nines;
    }
  }

  void multiplyPow5(int n) {
    while (n >= 8) {
      multiplySmall(390_625);
      n -= 8;
    }
    while (n > 0) {
      multiplySmall(5);
      n--;
    }
  }

  void multiplyPow2(int n) {
    while (n >= 10) {
      multiplySmall(1024);
      n -= 10;
    }
    while (n > 0) {
      multiplySmall(2);
      n--;
    }
  }

  void addAbsolute(DecNum other) {
    alignExponents(other);
    int n = Math.max(length, other.length);
    ensureRoom(n + 1);
    long carry = 0;
    for (int i = 0; i < n; i++) {
      long sum = carry + limb(i) + other.limb(i);
      limbs[i] = (int) (sum % BASE);
      carry = sum / BASE;
    }
    length = n;
    if (carry != 0) {
      limbs[length++] = (int) carry;
    }
  }

  void subtractAbsolute(DecNum other) {
    alignExponents(other);
    long borrow = 0;
    for (int i = 0; i < length; i++) {
      long diff = limb(i) - other.limb(i) - borrow;
      if (diff < 0) {
        diff += BASE;
        borrow = 1;
      } else {
        borrow = 0;
      }
      limbs[i] = (int) diff;
    }
    trim();
  }

  int compareAbsolute(DecNum other) {
    if (isZero() || other.isZero()) {
      return isZero() ? (other.isZero() ? 0 : -1) : 1;
    }
    int adjustedExponent = exp + digitCount();
    int otherAdjustedExponent = other.exp + other.digitCount();
    if (adjustedExponent != otherAdjustedExponent) {
      return Integer.compare(adjustedExponent, otherAdjustedExponent);
    }
    DecNum a = this;
    DecNum b = other;
    DecNum left = new DecNum();
    DecNum right = new DecNum();
    left.copyFrom(a);
    right.copyFrom(b);
    left.alignExponents(right);
    if (left.length != right.length) {
      return Integer.compare(left.length, right.length);
    }
    for (int i = left.length - 1; i >= 0; i--) {
      int cmp = Integer.compare(left.limbs[i], right.limbs[i]);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  void multiply(DecNum other) {
    if (isZero() || other.isZero()) {
      boolean resultNegative = negative ^ other.negative;
      int resultExponent = exp + other.exp;
      clear();
      negative = resultNegative;
      exp = resultExponent;
      return;
    }
    int[] product = new int[length + other.length + 1];
    for (int i = 0; i < length; i++) {
      long carry = 0;
      for (int j = 0; j < other.length; j++) {
        long acc = (long) limbs[i] * other.limbs[j] + product[i + j] + carry;
        product[i + j] = (int) (acc % BASE);
        carry = acc / BASE;
      }
      int k = i + other.length;
      while (carry != 0) {
        long acc = product[k] + carry;
        product[k] = (int) (acc % BASE);
        carry = acc / BASE;
        k++;
      }
    }
    negative ^= other.negative;
    exp += other.exp;
    ensureRoom(product.length);
    length = product.length;
    System.arraycopy(product, 0, limbs, 0, length);
    trim();
  }

  /**
   * Divides this integer by 10^{n}, returning the first discarded digit and
   * whether any further discarded digits were nonzero. {@code exp} is increased
   * by {@code n}.
   */
  int dividePow10(int n, boolean[] stickyOut) {
    if (n <= 0) {
      stickyOut[0] = false;
      return 0;
    }
    boolean sticky = false;
    int first = 0;
    for (int i = 0; i < n; i++) {
      if (first != 0) {
        sticky = true;
      }
      first = divideSmall(10);
    }
    exp += n;
    stickyOut[0] = sticky;
    return first;
  }

  int divideSmall(int divisor) {
    long remainder = 0;
    for (int i = length - 1; i >= 0; i--) {
      long current = remainder * BASE + limbs[i];
      limbs[i] = (int) (current / divisor);
      remainder = current % divisor;
    }
    trim();
    return (int) remainder;
  }

  boolean roundToDigits(int precision, RoundingMode mode, StatusFlags flags) {
    int digits = digitCount();
    if (isZero() || digits <= precision) {
      return false;
    }
    boolean[] sticky = {false};
    int first = dividePow10(digits - precision, sticky);
    boolean inexact = first != 0 || sticky[0];
    long low = low64();
    if (BidRound.shouldIncrement(negative, low, first, sticky[0], mode)) {
      addOne();
      if (digitCount() > precision) {
        dividePow10(1, sticky);
      }
    }
    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
    }
    return inexact;
  }

  void addOne() {
    long carry = 1;
    for (int i = 0; i < length && carry != 0; i++) {
      long sum = limbs[i] + carry;
      limbs[i] = (int) (sum % BASE);
      carry = sum / BASE;
    }
    if (carry != 0) {
      ensureRoom();
      limbs[length++] = (int) carry;
    }
  }

  static Sqrt sqrtFloor(DecNum input) {
    String digits = input.toDigits();
    DecNum root = new DecNum();
    DecNum remainder = new DecNum();
    int position = 0;
    int firstLength = (digits.length() & 1) == 0 ? 2 : 1;
    while (position < digits.length()) {
      int pairLength = position == 0 ? firstLength : 2;
      int pair = Integer.parseInt(digits.substring(position, position + pairLength));
      position += pairLength;
      remainder.multiplySmall(100);
      remainder.addDigit(pair);
      int selected = 0;
      DecNum selectedValue = new DecNum();
      for (int digit = 9; digit >= 0; digit--) {
        DecNum candidate = new DecNum();
        candidate.copyFrom(root);
        candidate.multiplySmall(20);
        candidate.addDigit(digit);
        candidate.multiplySmall(digit);
        if (candidate.compareAbsolute(remainder) <= 0) {
          selected = digit;
          selectedValue = candidate;
          break;
        }
      }
      root.multiplyBy10();
      root.addDigit(selected);
      remainder.subtractAbsolute(selectedValue);
    }
    return new Sqrt(root, remainder);
  }

  long low64() {
    long value = 0;
    long scale = 1;
    for (int i = 0; i < length; i++) {
      if (Long.compareUnsigned(scale, Long.divideUnsigned(-1L, BASE) + 1) > 0
          && i > 0) {
        break;
      }
      value += (long) limbs[i] * scale;
      if (i + 1 < length) {
        scale *= BASE;
      }
    }
    return value;
  }

  UInt128 toUInt128() {
    UInt128 value = UInt128.ZERO;
    for (int i = length - 1; i >= 0; i--) {
      value = value.multiply(BASE).add(limbs[i]);
    }
    return value;
  }

  java.math.BigInteger toBigIntegerAbsolute() {
    if (isZero()) {
      return java.math.BigInteger.ZERO;
    }
    return new java.math.BigInteger(toDigits());
  }

  String toDigits() {
    if (isZero()) {
      return "0";
    }
    StringBuilder builder = new StringBuilder();
    builder.append(limbs[length - 1]);
    for (int i = length - 2; i >= 0; i--) {
      String chunk = Integer.toString(limbs[i]);
      for (int pad = chunk.length(); pad < 9; pad++) {
        builder.append('0');
      }
      builder.append(chunk);
    }
    return builder.toString();
  }

  void stripTrailingZeros(int maximumExponent) {
    while (!isZero() && limbs[0] % 10 == 0 && exp < maximumExponent) {
      divideSmall(10);
      exp++;
    }
  }

  long packBid64(RoundingMode mode, StatusFlags flags) {
    boolean requiresExponentRounding = false;
    if (!isZero() && exp + 398 < 0) {
      DecNum precisionProbe = new DecNum();
      precisionProbe.copyFrom(this);
      precisionProbe.roundToDigits(16, mode, new StatusFlags());
      requiresExponentRounding = precisionProbe.exp + 398 < 0;
    }
    if (requiresExponentRounding) {
      int shift = Math.max(-(exp + 398), digitCount() - 16);
      DecNum scaled = new DecNum();
      scaled.copyFrom(this);
      boolean[] sticky = {false};
      int first = scaled.dividePow10(shift, sticky);
      long coefficient = scaled.low64();
      if (BidRound.shouldIncrement(negative, coefficient, first, sticky[0], mode)) {
        scaled.addOne();
        if (scaled.digitCount() > 16) {
          scaled.dividePow10(1, sticky);
        }
        coefficient = scaled.low64();
      }
      int biasedExponent = scaled.exp + 398;
      if (first != 0 || sticky[0]) {
        flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
      }
      return Bid64.finiteRawBits(negative, biasedExponent, coefficient);
    }
    boolean roundedInexact = roundToDigits(16, mode, flags);
    int unbiased = exp;
    long coeff = toUInt128().low();
    if (toUInt128().high() != 0L) {
      throw new IllegalStateException("BID64 coefficient overflow");
    }
    int biased = unbiased + 398;
    if (isZero()) {
      if (biased < 0) {
        biased = 0;
      }
      if (biased > 767) {
        biased = 767;
      }
      return Bid64.finiteRawBits(negative, biased, 0L);
    }
    if (biased < 0) {
      int shift = -biased;
      boolean[] sticky = {false};
      DecNum scaled = new DecNum();
      scaled.copyFrom(this);
      scaled.exp = 0;
      int first = scaled.dividePow10(shift, sticky);
      long c = scaled.low64();
      if (BidRound.shouldIncrement(negative, c, first, sticky[0], mode)) {
        c++;
      }
      if (first != 0 || sticky[0]) {
        flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
      }
      return Bid64.finiteRawBits(negative, 0, c);
    }
    while (biased > 767 && coeff <= PowersOfTen.MAX_16 / 10L) {
      coeff *= 10L;
      biased--;
    }
    if (biased > 767) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      if (BidRound.overflowToInfinity(negative, mode)) {
        return (negative ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_INFINITY;
      }
      return Bid64.finiteRawBits(negative, 767, PowersOfTen.MAX_16);
    }
    if (biased == 0 && coeff < PowersOfTen.LONG[15] && roundedInexact) {
      flags.raise(StatusFlags.UNDERFLOW);
    }
    return Bid64.finiteRawBits(negative, biased, coeff);
  }

  void packBid128(RoundingMode mode, StatusFlags flags, long[] payloadOut) {
    boolean requiresExponentRounding = false;
    if (!isZero() && exp + 6176 < 0) {
      DecNum precisionProbe = new DecNum();
      precisionProbe.copyFrom(this);
      precisionProbe.roundToDigits(34, mode, new StatusFlags());
      requiresExponentRounding = precisionProbe.exp + 6176 < 0;
    }
    if (requiresExponentRounding) {
      int shift = Math.max(-(exp + 6176), digitCount() - 34);
      DecNum scaled = new DecNum();
      scaled.copyFrom(this);
      boolean[] sticky = {false};
      int first = scaled.dividePow10(shift, sticky);
      if (BidRound.shouldIncrement(
          negative, scaled.low64(), first, sticky[0], mode)) {
        scaled.addOne();
        if (scaled.digitCount() > 34) {
          scaled.dividePow10(1, sticky);
        }
      }
      int biasedExponent = scaled.exp + 6176;
      UInt128 coefficient = scaled.toUInt128();
      if (first != 0 || sticky[0]) {
        flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
      }
      store128(
          Bid128.finite(negative, biasedExponent, coefficient.high(), coefficient.low()),
          payloadOut);
      return;
    }
    boolean roundedInexact = roundToDigits(34, mode, flags);
    int biased = exp + 6176;
    UInt128 coeff = toUInt128();
    if (isZero()) {
      if (biased < 0) {
        biased = 0;
      }
      if (biased > 12_287) {
        biased = 12_287;
      }
      store128(Bid128.finite(negative, biased, 0L, 0L), payloadOut);
      return;
    }
    if (biased < 0) {
      int shift = -biased;
      boolean[] sticky = {false};
      DecNum scaled = new DecNum();
      scaled.copyFrom(this);
      scaled.exp = 0;
      int first = scaled.dividePow10(shift, sticky);
      if (BidRound.shouldIncrement(
          negative, scaled.low64(), first, sticky[0], mode)) {
        scaled.addOne();
      }
      coeff = scaled.toUInt128();
      if (first != 0 || sticky[0]) {
        flags.raise(StatusFlags.UNDERFLOW | StatusFlags.INEXACT);
      }
      store128(Bid128.finite(negative, 0, coeff.high(), coeff.low()), payloadOut);
      return;
    }
    while (biased > 12_287 && coeff.compareTo(PowersOfTen.MAX_34.divide(10).quotient()) <= 0) {
      coeff = coeff.multiply(10L);
      biased--;
    }
    if (biased > 12_287) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      if (BidRound.overflowToInfinity(negative, mode)) {
        store128(negative ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY, payloadOut);
        return;
      }
      store128(
          Bid128.finite(negative, 12_287, PowersOfTen.MAX_34.high(), PowersOfTen.MAX_34.low()),
          payloadOut);
      return;
    }
    if (biased == 0 && coeff.compareTo(PowersOfTen.pow10(33)) < 0 && roundedInexact) {
      flags.raise(StatusFlags.UNDERFLOW);
    }
    store128(Bid128.finite(negative, biased, coeff.high(), coeff.low()), payloadOut);
  }

  static void store128(Bid128 value, long[] payloadOut) {
    payloadOut[0] = value.highBits();
    payloadOut[1] = value.lowBits();
  }

  static final class Sqrt {
    private final DecNum root;
    private final DecNum remainder;

    private Sqrt(DecNum root, DecNum remainder) {
      this.root = root;
      this.remainder = remainder;
    }

    DecNum root() {
      return root;
    }

    DecNum remainder() {
      return remainder;
    }
  }

  private void alignExponents(DecNum other) {
    if (exp == other.exp) {
      return;
    }
    if (exp > other.exp) {
      multiplyPow10(exp - other.exp);
      exp = other.exp;
    } else {
      other.multiplyPow10(other.exp - exp);
      other.exp = exp;
    }
  }

  private int limb(int index) {
    return index < length ? limbs[index] : 0;
  }

  private void trim() {
    while (length > 1 && limbs[length - 1] == 0) {
      length--;
    }
    if (isZero()) {
      negative = false;
    }
  }

  private void ensureRoom() {
    ensureRoom(length + 1);
  }

  private void ensureRoom(int needed) {
    if (needed > MAX_LIMBS) {
      throw new ArithmeticException("decimal coefficient exceeds conversion limit");
    }
    if (needed > limbs.length) {
      int capacity = limbs.length;
      while (capacity < needed) {
        capacity = Math.min(MAX_LIMBS, capacity * 2);
      }
      int[] expanded = new int[capacity];
      System.arraycopy(limbs, 0, expanded, 0, length);
      limbs = expanded;
    }
  }

  private static int pow10int(int n) {
    int value = 1;
    for (int i = 0; i < n; i++) {
      value *= 10;
    }
    return value;
  }

  private void shiftLeftBits(int bits) {
    for (int i = 0; i < bits; i++) {
      multiplySmall(2);
    }
  }
}
