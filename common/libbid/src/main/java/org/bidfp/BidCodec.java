/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

/**
 * Generic IEEE 754 BID combination-field encoder and decoder for any
 * {@link FormatParams} width.
 *
 * <p>Packed encodings are little-endian {@code long[]} of
 * {@link FormatParams#wordCount()} words. Unused high bits of the last word
 * are zero.
 */
final class BidCodec {
  private BidCodec() {
  }

  static void clear(FormatParams format, long[] words) {
    int n = format.wordCount();
    for (int i = 0; i < n; i++) {
      words[i] = 0L;
    }
  }

  static void packFinite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      UIntLimbs coefficient,
      long[] words) {
    packFinite(format, negative, biasedExponent, coefficient.limbs(), words);
  }

  static void packFinite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      Wide6 coefficient,
      long[] words) {
    clear(format, words);
    int n = format.wordCount();
    if (n > 0) {
      words[0] = coefficient.l0;
    }
    if (n > 1) {
      words[1] = coefficient.l1;
    }
    if (n > 2) {
      words[2] = coefficient.l2;
    }
    if (n > 3) {
      words[3] = coefficient.l3;
    }
    finishPackFinite(
        format, negative, biasedExponent, isForm1(format, coefficient), words);
  }

  static void packFinite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      long[] coefficient,
      long[] words) {
    clear(format, words);
    int n = Math.min(format.wordCount(), coefficient.length);
    System.arraycopy(coefficient, 0, words, 0, n);
    finishPackFinite(
        format, negative, biasedExponent, isForm1(format, coefficient), words);
  }

  private static void finishPackFinite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      boolean form1,
      long[] words) {
    int last = format.lastWord();
    long hi = words[last];
    long exp = ((long) biasedExponent & format.expMask());
    if (form1) {
      hi &= format.form1CoeffLastMask();
      hi |= exp << format.form1ExpShift();
    } else {
      hi &= format.form2CoeffLastMask();
      hi |= exp << format.form2ExpShift();
      hi |= format.steeringMask();
    }
    if (negative) {
      hi |= format.signMask();
    }
    words[last] = hi;
  }

  static void packInfinity(FormatParams format, boolean negative, long[] words) {
    clear(format, words);
    long hi = format.infGBits();
    if (negative) {
      hi |= format.signMask();
    }
    words[format.lastWord()] = hi;
  }

  static void packNaN(
      FormatParams format, boolean negative, boolean signaling, long[] words) {
    clear(format, words);
    long hi = format.nanBits();
    if (signaling) {
      hi |= format.snanMask();
    }
    if (negative) {
      hi |= format.signMask();
    }
    words[format.lastWord()] = hi;
  }

  static boolean isSigned(FormatParams format, long[] words) {
    return isSigned(format, words[format.lastWord()]);
  }

  static boolean isSigned(FormatParams format, long hi) {
    return (hi & format.signMask()) != 0L;
  }

  static boolean isNaN(FormatParams format, long[] words) {
    return isNaN(format, words[format.lastWord()]);
  }

  static boolean isNaN(FormatParams format, long hi) {
    return (hi & format.nanBits()) == format.nanBits();
  }

  static boolean isSignalingNaN(FormatParams format, long[] words) {
    return isSignalingNaN(format, words[format.lastWord()]);
  }

  static boolean isSignalingNaN(FormatParams format, long hi) {
    return isNaN(format, hi) && (hi & format.snanMask()) != 0L;
  }

  static boolean isInfinite(FormatParams format, long[] words) {
    return isInfinite(format, words[format.lastWord()]);
  }

  static boolean isInfinite(FormatParams format, long hi) {
    return (hi & format.nanBits()) == format.infGBits();
  }

  static boolean isFinite(FormatParams format, long[] words) {
    return isFinite(format, words[format.lastWord()]);
  }

  static boolean isFinite(FormatParams format, long hi) {
    return (hi & format.infGBits()) != format.infGBits();
  }

  static int unpackBiasedExponent(FormatParams format, long[] words) {
    return unpackBiasedExponent(format, words[format.lastWord()]);
  }

  static int unpackBiasedExponent(FormatParams format, long hi) {
    if ((hi & format.steeringMask()) == format.steeringMask()) {
      return (int) ((hi >>> format.form2ExpShift()) & format.expMask());
    }
    return (int) ((hi >>> format.form1ExpShift()) & format.expMask());
  }

  static void unpackCoefficient(FormatParams format, long[] words, UIntLimbs dest) {
    unpackCoefficient(format, words, dest.limbs());
  }

  static void unpackCoefficient(FormatParams format, long[] words, Wide6 dest) {
    dest.clear();
    int n = format.wordCount();
    if (n > 0) {
      dest.l0 = words[0];
    }
    if (n > 1) {
      dest.l1 = words[1];
    }
    if (n > 2) {
      dest.l2 = words[2];
    }
    if (n > 3) {
      dest.l3 = words[3];
    }
    applyCoeffMask(format, dest, words[format.lastWord()]);
  }

  static void unpackCoefficient(FormatParams format, long[] words, long[] dest) {
    for (int i = 0; i < dest.length; i++) {
      dest[i] = 0L;
    }
    System.arraycopy(words, 0, dest, 0, Math.min(words.length, dest.length));
    applyCoeffMask(format, dest, words[format.lastWord()]);
  }

  static void applyCoeffMask(FormatParams format, Wide6 dest, long hi) {
    int last = format.lastWord();
    long masked;
    if ((hi & format.steeringMask()) == format.steeringMask()) {
      masked = (hi & format.form2CoeffLastMask()) | format.form2ImplicitMask();
    } else {
      masked = hi & format.form1CoeffLastMask();
    }
    if (last == 0) {
      dest.l0 = masked;
    } else if (last == 1) {
      dest.l1 = masked;
    } else if (last == 2) {
      dest.l2 = masked;
    } else {
      dest.l3 = masked;
    }
  }

  static void applyCoeffMask(FormatParams format, long[] dest, long hi) {
    int last = format.lastWord();
    if ((hi & format.steeringMask()) == format.steeringMask()) {
      dest[last] = (hi & format.form2CoeffLastMask()) | format.form2ImplicitMask();
    } else {
      dest[last] = hi & format.form1CoeffLastMask();
    }
  }

  static void copyWords(FormatParams format, long[] src, long[] dest) {
    int n = format.wordCount();
    System.arraycopy(src, 0, dest, 0, n);
  }

  static long[] cloneWords(FormatParams format, long[] src) {
    long[] dest = new long[format.wordCount()];
    copyWords(format, src, dest);
    return dest;
  }

  static boolean isForm1(FormatParams format, Wide6 coefficient) {
    int last = format.lastWord();
    long extra = ~format.form1CoeffLastMask();
    if (last == 0) {
      return (coefficient.l0 & extra) == 0L
          && (coefficient.l1 | coefficient.l2 | coefficient.l3
          | coefficient.l4 | coefficient.l5) == 0L;
    }
    if (last == 1) {
      return (coefficient.l1 & extra) == 0L
          && (coefficient.l2 | coefficient.l3 | coefficient.l4
          | coefficient.l5) == 0L;
    }
    if (last == 2) {
      return (coefficient.l2 & extra) == 0L
          && (coefficient.l3 | coefficient.l4 | coefficient.l5) == 0L;
    }
    return (coefficient.l3 & extra) == 0L
        && (coefficient.l4 | coefficient.l5) == 0L;
  }

  static boolean isForm1(FormatParams format, long[] coefficient) {
    int last = format.lastWord();
    if ((coefficient[last] & ~format.form1CoeffLastMask()) != 0L) {
      return false;
    }
    for (int i = last + 1; i < coefficient.length; i++) {
      if (coefficient[i] != 0L) {
        return false;
      }
    }
    return true;
  }
}
