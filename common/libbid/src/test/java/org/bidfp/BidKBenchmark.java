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

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * BidK vs {@link BigDecimal} timings for Decimal160 and Decimal256.
 *
 * <p>Operand construction matches {@code BidJmhBenchmark}: sameQuantum,
 * mixedQuantum, and fullPrecision for add/mul/div; positive full-precision
 * values in {@code [1, 10)} for subtract, sqrt, FMA, and comparisons.
 *
 * <p>Run: {@code build/sbt "libbid/Test/runMain org.bidfp.BidKBenchmark full"}
 * Prints an object-API table then a {@link BidKRaw} table that writes into a
 * reused {@code out[]} buffer.
 */
public final class BidKBenchmark {
  private static final RoundingMode BID_ROUNDING = RoundingMode.TIES_TO_EVEN;
  private static final int SIZE = 1024;
  private static final int MASK = SIZE - 1;
  private static final FormatParams[] FORMATS = {
    FormatParams.DECIMAL160,
    FormatParams.DECIMAL256
  };

  private BidKBenchmark() {
  }

  public static void main(String[] args) {
    Profile profile = Profile.from(args.length == 0 ? "quick" : args[0]);
    System.out.println("BidKBenchmark profile=" + profile.name);
    System.out.println();
    printTable("BidK object API", false);
    System.out.println();
    printTable("BidKRaw into reused out[]", true);
  }

  private static void printTable(String title, boolean raw) {
    System.out.println(title);
    System.out.println(
        "| Function / workload | BID160 | BD160 | Ratio | BID256 | BD256 | Ratio |");
    System.out.println(
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
    List<Row> rows = new ArrayList<>();
    rows.add(measurePair("add (same quantum)", "sameQuantum", Op.ADD, raw));
    rows.add(measurePair("add (mixed quantum)", "mixedQuantum", Op.ADD, raw));
    rows.add(measurePair("add (full precision)", "fullPrecision", Op.ADD, raw));
    rows.add(measurePair("subtract", "unit", Op.SUB, raw));
    rows.add(measurePair("multiply (same quantum)", "sameQuantum", Op.MUL, raw));
    rows.add(measurePair("multiply (mixed quantum)", "mixedQuantum", Op.MUL, raw));
    rows.add(measurePair("multiply (full precision)", "fullPrecision", Op.MUL, raw));
    rows.add(measurePair("divide (same quantum)", "sameQuantum", Op.DIV, raw));
    rows.add(measurePair("divide (mixed quantum)", "mixedQuantum", Op.DIV, raw));
    rows.add(measurePair("divide (full precision)", "fullPrecision", Op.DIV, raw));
    rows.add(measurePair("square root", "unit", Op.SQRT, raw));
    rows.add(measurePair("fused multiply-add", "unit", Op.FMA, raw));
    rows.add(measurePair("ordered less-than", "unit", Op.LT, raw));
    rows.add(measurePair("cohort-equal numeric comparison", "unit", Op.EQ, raw));
    for (Row row : rows) {
      System.out.println(row.format());
    }
  }

  private static Row measurePair(
      String name, String workload, Op op, boolean raw) {
    Sample[] samples = new Sample[FORMATS.length];
    for (int i = 0; i < FORMATS.length; i++) {
      samples[i] = measure(FORMATS[i], workload, op, Profile.current, raw);
    }
    return new Row(name, samples);
  }

  private static Sample measure(
      FormatParams format,
      String workload,
      Op op,
      Profile profile,
      boolean raw) {
    State state = new State(format, workload);
    MathContext context = new MathContext(
        format.precision(), java.math.RoundingMode.HALF_EVEN);
    double bidNs = time(profile, () -> {
      if (raw) {
        op.runRaw(state);
      } else {
        op.runBid(state);
      }
    });
    double bdNs = time(profile, () -> op.runDecimal(state, context));
    return new Sample(bidNs, bdNs);
  }

  private static double time(Profile profile, Runnable body) {
    for (int i = 0; i < profile.warmup; i++) {
      runSlice(profile.sliceNanos, body);
    }
    double sum = 0.0;
    for (int i = 0; i < profile.measure; i++) {
      sum += runSlice(profile.sliceNanos, body);
    }
    return sum / profile.measure;
  }

  private static double runSlice(long sliceNanos, Runnable body) {
    long ops = 0L;
    long start = System.nanoTime();
    long deadline = start + sliceNanos;
    do {
      for (int i = 0; i < 16; i++) {
        body.run();
      }
      ops += 16L;
    } while (System.nanoTime() < deadline);
    long elapsed = System.nanoTime() - start;
    return (double) elapsed / (double) ops;
  }

  private enum Op {
    ADD {
      @Override
      void runBid(State state) {
        int i = state.next();
        state.sink ^= state.left[i].add(
            state.right[i], BID_ROUNDING, state.flags).hashCode();
      }

      @Override
      void runRaw(State state) {
        int i = state.next();
        BidKRaw.add(
            state.format, state.leftBits[i], state.rightBits[i],
            BID_ROUNDING, state.flags, state.out);
        xorOut(state);
      }

      @Override
      void runDecimal(State state, MathContext context) {
        int i = state.next();
        state.sink ^= state.leftBd[i].add(state.rightBd[i], context).hashCode();
      }
    },
    SUB {
      @Override
      void runBid(State state) {
        int i = state.next();
        state.sink ^= state.left[i].subtract(
            state.right[i], BID_ROUNDING, state.flags).hashCode();
      }

      @Override
      void runRaw(State state) {
        int i = state.next();
        BidKRaw.sub(
            state.format, state.leftBits[i], state.rightBits[i],
            BID_ROUNDING, state.flags, state.out);
        xorOut(state);
      }

      @Override
      void runDecimal(State state, MathContext context) {
        int i = state.next();
        state.sink ^= state.leftBd[i].subtract(state.rightBd[i], context).hashCode();
      }
    },
    MUL {
      @Override
      void runBid(State state) {
        int i = state.next();
        state.sink ^= state.left[i].multiply(
            state.right[i], BID_ROUNDING, state.flags).hashCode();
      }

      @Override
      void runRaw(State state) {
        int i = state.next();
        BidKRaw.mul(
            state.format, state.leftBits[i], state.rightBits[i],
            BID_ROUNDING, state.flags, state.out);
        xorOut(state);
      }

      @Override
      void runDecimal(State state, MathContext context) {
        int i = state.next();
        state.sink ^= state.leftBd[i].multiply(state.rightBd[i], context).hashCode();
      }
    },
    DIV {
      @Override
      void runBid(State state) {
        int i = state.next();
        state.sink ^= state.left[i].divide(
            state.right[i], BID_ROUNDING, state.flags).hashCode();
      }

      @Override
      void runRaw(State state) {
        int i = state.next();
        BidKRaw.div(
            state.format, state.leftBits[i], state.rightBits[i],
            BID_ROUNDING, state.flags, state.out);
        xorOut(state);
      }

      @Override
      void runDecimal(State state, MathContext context) {
        int i = state.next();
        state.sink ^= state.leftBd[i].divide(state.rightBd[i], context).hashCode();
      }
    },
    SQRT {
      @Override
      void runBid(State state) {
        int i = state.next();
        state.sink ^= state.left[i].sqrt(BID_ROUNDING, state.flags).hashCode();
      }

      @Override
      void runRaw(State state) {
        int i = state.next();
        BidKRaw.sqrt(
            state.format, state.leftBits[i], BID_ROUNDING, state.flags, state.out);
        xorOut(state);
      }

      @Override
      void runDecimal(State state, MathContext context) {
        int i = state.next();
        state.sink ^= state.leftBd[i].sqrt(context).hashCode();
      }
    },
    FMA {
      @Override
      void runBid(State state) {
        int i = state.next();
        state.sink ^= state.left[i].fma(
            state.right[i], state.addend[i], BID_ROUNDING, state.flags).hashCode();
      }

      @Override
      void runRaw(State state) {
        int i = state.next();
        BidKRaw.fma(
            state.format, state.leftBits[i], state.rightBits[i], state.addendBits[i],
            BID_ROUNDING, state.flags, state.out);
        xorOut(state);
      }

      @Override
      void runDecimal(State state, MathContext context) {
        int i = state.next();
        state.sink ^= state.leftBd[i].multiply(state.rightBd[i])
            .add(state.addendBd[i]).round(context).hashCode();
      }
    },
    LT {
      @Override
      void runBid(State state) {
        int i = state.next();
        state.sink ^= state.left[i].sqlCompare(state.right[i]);
      }

      @Override
      void runRaw(State state) {
        int i = state.next();
        state.sink ^= BidKRaw.sqlCompare(
            state.format, state.leftBits[i], state.rightBits[i]);
      }

      @Override
      void runDecimal(State state, MathContext context) {
        int i = state.next();
        state.sink ^= Integer.compare(state.leftBd[i].compareTo(state.rightBd[i]), 0);
      }
    },
    EQ {
      @Override
      void runBid(State state) {
        int i = state.next();
        state.sink ^= Boolean.hashCode(state.left[i].sqlEquals(state.right[i]));
      }

      @Override
      void runRaw(State state) {
        int i = state.next();
        state.sink ^= Boolean.hashCode(
            BidKRaw.sqlCompare(state.format, state.leftBits[i], state.rightBits[i])
                == 0);
      }

      @Override
      void runDecimal(State state, MathContext context) {
        int i = state.next();
        state.sink ^= Boolean.hashCode(state.leftBd[i].compareTo(state.rightBd[i]) == 0);
      }
    };

    abstract void runBid(State state);

    abstract void runRaw(State state);

    abstract void runDecimal(State state, MathContext context);

    static void xorOut(State state) {
      long[] out = state.out;
      for (int i = 0; i < out.length; i++) {
        state.sink ^= out[i];
      }
    }
  }

  private static final class State {
    final FormatParams format;
    final BidK[] left;
    final BidK[] right;
    final BidK[] addend;
    final long[][] leftBits;
    final long[][] rightBits;
    final long[][] addendBits;
    final long[] out;
    final BigDecimal[] leftBd;
    final BigDecimal[] rightBd;
    final BigDecimal[] addendBd;
    final StatusFlags flags = new StatusFlags();
    int index;
    long sink = Long.MIN_VALUE;

    State(FormatParams format, String workload) {
      this.format = format;
      left = new BidK[SIZE];
      right = new BidK[SIZE];
      addend = new BidK[SIZE];
      leftBits = new long[SIZE][];
      rightBits = new long[SIZE][];
      addendBits = new long[SIZE][];
      out = new long[format.wordCount()];
      leftBd = new BigDecimal[SIZE];
      rightBd = new BigDecimal[SIZE];
      addendBd = new BigDecimal[SIZE];
      Random random = new Random(0x5eed_b1dL ^ format.packedBits() ^ workload.hashCode());
      int digits = format.precision();
      for (int i = 0; i < SIZE; i++) {
        String leftText;
        String rightText;
        String addendText;
        if ("unit".equals(workload)) {
          leftText = unitText(random, digits, false);
          rightText = unitText(random, digits, false);
          addendText = unitText(random, digits, true);
        } else {
          Pair pair = pair(random, digits, workload);
          leftText = pair.left;
          rightText = pair.right;
          addendText = pair.right;
        }
        left[i] = BidK.parse(format, leftText);
        right[i] = BidK.parse(format, rightText);
        addend[i] = BidK.parse(format, addendText);
        leftBits[i] = left[i].toRawBits();
        rightBits[i] = right[i].toRawBits();
        addendBits[i] = addend[i].toRawBits();
        leftBd[i] = new BigDecimal(leftText);
        rightBd[i] = new BigDecimal(rightText);
        addendBd[i] = new BigDecimal(addendText);
      }
    }

    int next() {
      return index++ & MASK;
    }
  }

  private static Pair pair(Random random, int formatDigits, String workload) {
    int leftDigits;
    int rightDigits;
    int leftExponent;
    int rightExponent;
    if ("sameQuantum".equals(workload)) {
      leftDigits = Math.max(4, formatDigits - 6);
      rightDigits = leftDigits;
      leftExponent = randomExponent(random, 40);
      rightExponent = leftExponent;
    } else if ("mixedQuantum".equals(workload)) {
      leftDigits = randomDigits(random, 4, formatDigits - 4);
      rightDigits = randomDigits(random, 4, formatDigits - 4);
      leftExponent = randomExponent(random, 80);
      rightExponent = differentExponent(random, leftExponent, 80);
    } else if ("fullPrecision".equals(workload)) {
      leftDigits = formatDigits;
      rightDigits = formatDigits;
      leftExponent = randomExponent(random, 100);
      rightExponent = differentExponent(random, leftExponent, 100);
    } else {
      throw new IllegalArgumentException(workload);
    }
    return new Pair(
        decimalText(random, leftDigits, leftExponent),
        decimalText(random, rightDigits, rightExponent));
  }

  private static String unitText(Random random, int digits, boolean signed) {
    StringBuilder text = new StringBuilder(digits + 8);
    if (signed && random.nextBoolean()) {
      text.append('-');
    }
    text.append(random.nextInt(9) + 1);
    for (int i = 1; i < digits; i++) {
      text.append(random.nextInt(10));
    }
    return text.append('E').append(-(digits - 1)).toString();
  }

  private static String decimalText(Random random, int digits, int exponent) {
    StringBuilder text = new StringBuilder(digits + 8);
    if (random.nextBoolean()) {
      text.append('-');
    }
    text.append(random.nextInt(9) + 1);
    for (int i = 1; i < digits; i++) {
      text.append(random.nextInt(10));
    }
    return text.append('E').append(exponent).toString();
  }

  private static int randomDigits(Random random, int minimum, int maximum) {
    return minimum + random.nextInt(maximum - minimum + 1);
  }

  private static int randomExponent(Random random, int magnitude) {
    return random.nextInt(2 * magnitude + 1) - magnitude;
  }

  private static int differentExponent(Random random, int excluded, int magnitude) {
    int exponent;
    do {
      exponent = randomExponent(random, magnitude);
    } while (exponent == excluded);
    return exponent;
  }

  private static final class Pair {
    final String left;
    final String right;

    Pair(String left, String right) {
      this.left = left;
      this.right = right;
    }
  }

  private static final class Sample {
    final double bidNs;
    final double bdNs;

    Sample(double bidNs, double bdNs) {
      this.bidNs = bidNs;
      this.bdNs = bdNs;
    }
  }

  private static final class Row {
    final String name;
    final Sample[] samples;

    Row(String name, Sample[] samples) {
      this.name = name;
      this.samples = samples;
    }

    String format() {
      StringBuilder line = new StringBuilder("| ").append(name);
      for (Sample sample : samples) {
        line.append(" | ").append(ns(sample.bidNs));
        line.append(" | ").append(ns(sample.bdNs));
        line.append(" | ").append(ratio(sample.bdNs, sample.bidNs));
      }
      line.append(" |");
      return line.toString();
    }

    private static String ns(double value) {
      return String.format(Locale.ROOT, "%.2f", value);
    }

    private static String ratio(double bd, double bid) {
      return String.format(Locale.ROOT, "%.2fx", bd / bid);
    }
  }

  private static final class Profile {
    static Profile current;

    final String name;
    final int warmup;
    final int measure;
    final long sliceNanos;

    private Profile(String name, int warmup, int measure, long sliceNanos) {
      this.name = name;
      this.warmup = warmup;
      this.measure = measure;
      this.sliceNanos = sliceNanos;
    }

    static Profile from(String name) {
      Profile profile;
      switch (name) {
        case "iter":
          profile = new Profile("iter", 1, 2, 200_000_000L);
          break;
        case "quick":
          profile = new Profile("quick", 2, 3, 250_000_000L);
          break;
        case "full":
          profile = new Profile("full", 3, 5, 1_000_000_000L);
          break;
        default:
          throw new IllegalArgumentException("profile must be iter, quick, or full");
      }
      current = profile;
      return profile;
    }
  }
}
