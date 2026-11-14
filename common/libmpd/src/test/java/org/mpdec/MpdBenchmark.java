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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * LibMPD vs {@link BigDecimal} timings for DECFLOAT-relevant precisions.
 *
 * <p>Run: {@code build/sbt "libmpd/Test/runMain org.mpdec.MpdBenchmark quick"}
 */
public final class MpdBenchmark {
  private static final int SIZE = 1024;
  private static final int MASK = SIZE - 1;
  private static final int[] DEFAULT_PRECS = {7, 16, 25, 34, 43, 52, 61, 70};

  private MpdBenchmark() {
  }

  public static void main(String[] args) {
    String profileName = "quick";
    int only = 0;
    boolean withBid = false;
    for (String arg : args) {
      if (arg.startsWith("p=")) {
        only = Integer.parseInt(arg.substring(2));
      } else if ("bid".equals(arg)) {
        withBid = true;
      } else if ("quick".equals(arg) || "iter".equals(arg) || "full".equals(arg)) {
        profileName = arg;
      }
    }
    Profile profile = Profile.from(profileName);
    int[] precs = only == 0 ? DEFAULT_PRECS : new int[] {only};
    System.out.println("MpdBenchmark profile=" + profile.name
        + (withBid ? " bid" : ""));
    System.out.println();
    printTable(precs, profile, withBid);
  }

  private static void printTable(int[] precs, Profile profile, boolean withBid) {
    System.out.println("BD/MPD ratio per DECFLOAT width (>1 = MPD faster)");
    StringBuilder header = new StringBuilder("| Function / workload");
    StringBuilder rule = new StringBuilder("| ---");
    for (int p : precs) {
      header.append(" | p=").append(p);
      rule.append(" | ---:");
    }
    header.append(" |");
    rule.append(" |");
    System.out.println(header);
    System.out.println(rule);

    List<Row> rows = new ArrayList<>();
    rows.add(measure("add (same quantum)", "sameQuantum",
        Op.ADD, precs, profile, withBid));
    rows.add(measure("add (mixed quantum)", "mixedQuantum",
        Op.ADD, precs, profile, withBid));
    rows.add(measure("add (full precision)", "fullPrecision",
        Op.ADD, precs, profile, withBid));
    rows.add(measure("subtract", "unit",
        Op.SUB, precs, profile, withBid));
    rows.add(measure("multiply (same quantum)", "sameQuantum",
        Op.MUL, precs, profile, withBid));
    rows.add(measure("multiply (mixed quantum)", "mixedQuantum",
        Op.MUL, precs, profile, withBid));
    rows.add(measure("multiply (full precision)", "fullPrecision",
        Op.MUL, precs, profile, withBid));
    rows.add(measure("divide (same quantum)", "sameQuantum",
        Op.DIV, precs, profile, withBid));
    rows.add(measure("divide (mixed quantum)", "mixedQuantum",
        Op.DIV, precs, profile, withBid));
    rows.add(measure("divide (full precision)", "fullPrecision",
        Op.DIV, precs, profile, withBid));
    rows.add(measure("square root", "unit",
        Op.SQRT, precs, profile, withBid));
    rows.add(measure("fused multiply-add", "unit",
        Op.FMA, precs, profile, withBid));
    rows.add(measure("ordered less-than", "unit",
        Op.LT, precs, profile, withBid));
    rows.add(measure("cohort-equal numeric comparison", "unit",
        Op.EQ, precs, profile, withBid));
    for (Row row : rows) {
      System.out.println(row.format());
    }
  }

  private static Row measure(
      String name, String workload, Op op, int[] precs, Profile profile,
      boolean withBid) {
    Sample[] samples = new Sample[precs.length];
    for (int i = 0; i < precs.length; i++) {
      samples[i] = measureOne(precs[i], workload, op, profile, withBid);
    }
    return new Row(name, samples);
  }

  private static Sample measureOne(
      int prec, String workload, Op op, Profile profile, boolean withBid) {
    State state = new State(prec, workload);
    MathContext mc = new MathContext(prec, RoundingMode.HALF_EVEN);
    double mpdNs = time(profile, () -> op.runMpd(state));
    double bdNs = time(profile, () -> op.runBd(state, mc));
    Double bidNs = null;
    if (withBid && (prec == 16 || prec == 34) && op.supportsBid()) {
      BidBridge bridge = BidBridge.tryLoad(prec);
      if (bridge.available()) {
        try {
          state.loadBid(bridge);
          bidNs = time(profile, () -> {
            try {
              op.runBid(state, bridge);
            } catch (ReflectiveOperationException e) {
              throw new RuntimeException(e);
            }
          });
        } catch (ReflectiveOperationException e) {
          System.out.println("BID load failed for p=" + prec + ": " + e);
        }
      } else if (withBid) {
        System.out.println(
            "BID jar not on classpath for p=" + prec + "; skip BID column");
      }
    }
    boolean bidSlot = withBid && (prec == 16 || prec == 34);
    return new Sample(mpdNs, bdNs, bidNs, bidSlot);
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
      @Override void runMpd(State s) {
        int i = s.next();
        MpdOps.add(s.out, s.left[i], s.right[i], s.ctx);
        s.sink ^= s.out.digits;
      }
      @Override void runBd(State s, MathContext mc) {
        int i = s.next();
        s.bdSink = s.bdLeft[i].add(s.bdRight[i], mc);
      }
      @Override void runBid(State s, BidBridge bridge)
          throws ReflectiveOperationException {
        int i = s.next();
        s.bidSink = bridge.add(s.bidLeft[i], s.bidRight[i], s.bidFlags);
      }
    },
    SUB {
      @Override void runMpd(State s) {
        int i = s.next();
        MpdOps.sub(s.out, s.left[i], s.right[i], s.ctx);
        s.sink ^= s.out.digits;
      }
      @Override void runBd(State s, MathContext mc) {
        int i = s.next();
        s.bdSink = s.bdLeft[i].subtract(s.bdRight[i], mc);
      }
      @Override void runBid(State s, BidBridge bridge)
          throws ReflectiveOperationException {
        int i = s.next();
        s.bidSink = bridge.sub(s.bidLeft[i], s.bidRight[i], s.bidFlags);
      }
    },
    MUL {
      @Override void runMpd(State s) {
        int i = s.next();
        MpdOps.mul(s.out, s.left[i], s.right[i], s.ctx);
        s.sink ^= s.out.digits;
      }
      @Override void runBd(State s, MathContext mc) {
        int i = s.next();
        s.bdSink = s.bdLeft[i].multiply(s.bdRight[i], mc);
      }
      @Override void runBid(State s, BidBridge bridge)
          throws ReflectiveOperationException {
        int i = s.next();
        s.bidSink = bridge.mul(s.bidLeft[i], s.bidRight[i], s.bidFlags);
      }
    },
    DIV {
      @Override void runMpd(State s) {
        int i = s.next();
        MpdOps.div(s.out, s.left[i], s.right[i], s.ctx);
        s.sink ^= s.out.digits;
      }
      @Override void runBd(State s, MathContext mc) {
        int i = s.next();
        s.bdSink = s.bdLeft[i].divide(s.bdRight[i], mc);
      }
      @Override void runBid(State s, BidBridge bridge)
          throws ReflectiveOperationException {
        int i = s.next();
        s.bidSink = bridge.div(s.bidLeft[i], s.bidRight[i], s.bidFlags);
      }
    },
    SQRT {
      @Override void runMpd(State s) {
        int i = s.next();
        MpdOps.sqrt(s.out, s.left[i], s.ctx);
        s.sink ^= s.out.digits;
      }
      @Override void runBd(State s, MathContext mc) {
        int i = s.next();
        s.bdSink = s.bdLeft[i].sqrt(mc);
      }
      @Override void runBid(State s, BidBridge bridge)
          throws ReflectiveOperationException {
        int i = s.next();
        s.bidSink = bridge.sqrt(s.bidLeft[i], s.bidFlags);
      }
    },
    FMA {
      @Override void runMpd(State s) {
        int i = s.next();
        MpdOps.fma(s.out, s.left[i], s.right[i], s.third[i], s.ctx);
        s.sink ^= s.out.digits;
      }
      @Override void runBd(State s, MathContext mc) {
        int i = s.next();
        s.bdSink = s.bdLeft[i].multiply(s.bdRight[i])
            .add(s.bdThird[i], mc);
      }
      @Override boolean supportsBid() { return false; }
    },
    LT {
      @Override void runMpd(State s) {
        int i = s.next();
        s.sink ^= MpdOps.compare(s.left[i], s.right[i]);
      }
      @Override void runBd(State s, MathContext mc) {
        int i = s.next();
        s.sink ^= s.bdLeft[i].compareTo(s.bdRight[i]);
      }
      @Override boolean supportsBid() { return false; }
    },
    EQ {
      @Override void runMpd(State s) {
        int i = s.next();
        s.sink ^= MpdOps.compare(s.left[i], s.right[i]) == 0 ? 1 : 0;
      }
      @Override void runBd(State s, MathContext mc) {
        int i = s.next();
        s.sink ^= s.bdLeft[i].compareTo(s.bdRight[i]) == 0 ? 1 : 0;
      }
      @Override boolean supportsBid() { return false; }
    };

    abstract void runMpd(State s);
    abstract void runBd(State s, MathContext mc);
    boolean supportsBid() { return true; }
    void runBid(State s, BidBridge bridge)
        throws ReflectiveOperationException {
      throw new UnsupportedOperationException();
    }
  }

  private static final class State {
    final MpdContext ctx;
    final Mpd[] left = new Mpd[SIZE];
    final Mpd[] right = new Mpd[SIZE];
    final Mpd[] third = new Mpd[SIZE];
    final BigDecimal[] bdLeft = new BigDecimal[SIZE];
    final BigDecimal[] bdRight = new BigDecimal[SIZE];
    final BigDecimal[] bdThird = new BigDecimal[SIZE];
    final Mpd out = new Mpd();
    Object[] bidLeft;
    Object[] bidRight;
    Object bidFlags;
    Object bidSink;
    final String[] leftText = new String[SIZE];
    final String[] rightText = new String[SIZE];
    int idx;
    int sink;
    BigDecimal bdSink = BigDecimal.ZERO;

    State(int prec, String workload) {
      this.ctx = MpdContext.ieee(prec);
      Random rnd = new Random(42L + prec + workload.hashCode());
      for (int i = 0; i < SIZE; i++) {
        BigDecimal a = operand(rnd, prec, workload, true);
        BigDecimal b = operand(rnd, prec, workload, false);
        BigDecimal c = unitOperand(rnd, prec);
        bdLeft[i] = a;
        bdRight[i] = b.signum() == 0 ? BigDecimal.ONE : b;
        bdThird[i] = c;
        leftText[i] = a.toEngineeringString();
        rightText[i] = bdRight[i].toEngineeringString();
        left[i] = MpdFormat.parse(leftText[i]);
        right[i] = MpdFormat.parse(rightText[i]);
        third[i] = MpdFormat.parse(c.toEngineeringString());
      }
    }

    void loadBid(BidBridge bridge) throws ReflectiveOperationException {
      bidLeft = bridge.parseAll(leftText);
      bidRight = bridge.parseAll(rightText);
      bidFlags = bridge.newFlags();
    }

    int next() {
      int i = idx;
      idx = (idx + 1) & MASK;
      return i;
    }
  }

  private static BigDecimal operand(
      Random rnd, int prec, String workload, boolean left) {
    return switch (workload) {
      case "sameQuantum" -> sameQuantum(rnd, prec);
      case "mixedQuantum" -> mixedQuantum(rnd, prec, left);
      case "fullPrecision" -> fullPrecision(rnd, prec);
      default -> unitOperand(rnd, prec);
    };
  }

  private static BigDecimal sameQuantum(Random rnd, int prec) {
    int digits = Math.max(2, prec / 2);
    return randomDigits(rnd, digits, -digits / 2);
  }

  private static BigDecimal mixedQuantum(Random rnd, int prec, boolean left) {
    int digits = 2 + rnd.nextInt(Math.max(2, prec - 1));
    int exp = left ? rnd.nextInt(13) - 6 : rnd.nextInt(21) - 20;
    return randomDigits(rnd, digits, exp);
  }

  private static BigDecimal fullPrecision(Random rnd, int prec) {
    return randomDigits(rnd, prec, -rnd.nextInt(prec));
  }

  private static BigDecimal unitOperand(Random rnd, int prec) {
    // positive value in [1, 10) with full precision
    return randomDigits(rnd, prec, 1 - prec).abs();
  }

  private static BigDecimal randomDigits(Random rnd, int digits, int exp) {
    StringBuilder sb = new StringBuilder(digits + 8);
    if (rnd.nextBoolean()) {
      sb.append('-');
    }
    sb.append(rnd.nextInt(9) + 1);
    for (int i = 1; i < digits; i++) {
      sb.append(rnd.nextInt(10));
    }
    sb.append('E').append(exp);
    return new BigDecimal(sb.toString());
  }

  private static final class Profile {
    final String name;
    final int warmup;
    final int measure;
    final long sliceNanos;

    Profile(String name, int warmup, int measure, long sliceNanos) {
      this.name = name;
      this.warmup = warmup;
      this.measure = measure;
      this.sliceNanos = sliceNanos;
    }

    static Profile from(String name) {
      return switch (name) {
        case "iter" -> new Profile("iter", 1, 2, 50_000_000L);
        case "full" -> new Profile("full", 3, 5, 1_000_000_000L);
        default -> new Profile("quick", 1, 2, 100_000_000L);
      };
    }
  }

  private static final class Sample {
    final double mpdNs;
    final double bdNs;
    final Double bidNs;
    final boolean bidSlot;

    Sample(double mpdNs, double bdNs, Double bidNs, boolean bidSlot) {
      this.mpdNs = mpdNs;
      this.bdNs = bdNs;
      this.bidNs = bidNs;
      this.bidSlot = bidSlot;
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
      StringBuilder sb = new StringBuilder("| ").append(name);
      for (Sample s : samples) {
        double ratio = s.mpdNs == 0.0 ? 0.0 : s.bdNs / s.mpdNs;
        sb.append(String.format(Locale.ROOT, " | %.2fx", ratio));
      }
      sb.append(" |");
      return sb.toString();
    }
  }
}
