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
package org.mpdec;

/**
 * Working context for quiet MPD operations.
 *
 * <p>Precision is capped at {@link Mpd#MAX_DIGITS}. IEEE-like presets are
 * available through {@link #ieee(int)}.
 */
public final class MpdContext {
  public int prec;
  public int emax;
  public int emin;
  public MpdRound round;
  public int clamp;
  public final MpdStatus status = new MpdStatus();

  public MpdContext(int prec, int emax, int emin, MpdRound round, int clamp) {
    if (prec < 1 || prec > Mpd.MAX_DIGITS) {
      throw new IllegalArgumentException("prec out of range: " + prec);
    }
    this.prec = prec;
    this.emax = emax;
    this.emin = emin;
    this.round = round;
    this.clamp = clamp;
  }

  public MpdContext copy() {
    return new MpdContext(prec, emax, emin, round, clamp);
  }

  /**
   * IEEE 754 decimal presets for common DECFLOAT widths:
   * 16 (decimal64), 34 (decimal128), 70 (decimal256).
   */
  public static MpdContext ieee(int precision) {
    return switch (precision) {
      case 16 -> new MpdContext(16, 384, -383, MpdRound.HALF_EVEN, 1);
      case 34 -> new MpdContext(34, 6144, -6143, MpdRound.HALF_EVEN, 1);
      case 70 -> new MpdContext(70, 1572864, -1572863, MpdRound.HALF_EVEN, 1);
      default -> {
        int emax = Math.max(999999, precision * 1000);
        yield new MpdContext(
            precision, emax, 1 - emax, MpdRound.HALF_EVEN, 1);
      }
    };
  }
}
