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

/**
 * Raw BID64 vectors copied from Intel RDFP 2.0 U4 TESTS/readtest.in.
 *
 * <p>The expected class numbers are the ordinals of Intel's {@code class_t}
 * enum and intentionally match {@link DecimalClass}.
 */
public final class Bid64IntelVectorTest {
  private static final long[][] CLASS_VECTORS = {
    {0x0000_0000_4010_0000L, 7},
    {0x069a_36c8_f9c0_9818L, 8},
    {0x1000_0000_0000_0000L, 6},
    {0x18eb_f0eb_8402_7607L, 8},
    {0x2407_f597_2ed7_b76bL, 8},
    {0x3e2a_85a9_8df1_a62eL, 8},
    {0x72bb_05ed_5f21_7403L, 8},
    {0x73dc_9f0c_efef_8aefL, 6},
    {0x74a2_3c8f_2229_106aL, 8},
    {0x79ff_fade_57fa_bf5bL, 9},
    {0x8c32_d9f8_3027_0c43L, 3},
    {0x97c4_180a_6666_476dL, 3},
    {0xa524_7757_cbba_3ba7L, 3},
    {0xb61b_1006_97ed_86c3L, 3},
    {0xb6ee_2fa4_0685_19a5L, 3},
    {0xc1e5_0e97_c4a1_cb96L, 3},
    {0xf0c3_ae21_3f30_b8d8L, 5},
    {0xfbff_feff_ffff_feffL, 2},
    {0xfde7_bf7f_3d7b_afd8L, 1},
    {0xfffe_ffff_ffff_feffL, 0}
  };

  private static final long[][] CANONICAL_VECTORS = {
    {0x2811_0864_1902_7808L, 1},
    {0x292f_c3f6_cda1_8948L, 1},
    {0x5473_faef_406b_0fcdL, 1},
    {0x68e3_4354_09c4_8c01L, 1},
    {0x75ab_31f5_7ab7_09ddL, 1},
    {0x79d0_6c1d_e91c_42f2L, 0},
    {0x7e03_f3e1_e4c8_5d17L, 0},
    {0xa802_0031_13f8_e3a6L, 1},
    {0xb58b_fd88_21f2_492aL, 1},
    {0xb9a0_e346_782c_737fL, 1},
    {0xc1ee_f726_24bc_f3e9L, 1},
    {0xc229_31d2_363b_48daL, 1},
    {0xddc2_8edd_86a0_e0dcL, 1},
    {0xe125_a4d6_9af5_06a9L, 0},
    {0xf800_0000_0000_0000L, 1},
    {0xf800_0000_0100_0000L, 0},
    {0xf810_0000_0000_0000L, 0},
    {0xfe00_c3d1_09f0_dda6L, 1},
    {0xffff_feff_ffff_ffffL, 0}
  };

  private static final long[][] SAME_QUANTUM_VECTORS = {
    {0x0000_0000_0000_0000L, 0x698d_80a4_efe3_3f69L, 0},
    {0x29c9_5cda_703c_6a50L, 0x97db_1cf7_f4ee_8859L, 0},
    {0x2dcb_525f_907d_ee77L, 0xd935_46fb_edfc_552eL, 0},
    {0x4718_50f1_3894_2eebL, 0x5d7c_6bc1_58b0_b732L, 0},
    {0x7c00_0000_0000_0000L, 0xfef3_ffff_ffff_ffffL, 1},
    {0x7e00_0000_0000_0000L, 0x7cf3_ffff_ffff_ffffL, 1},
    {0x7e26_97cc_ba10_d426L, 0xfa25_7179_2acb_c717L, 0},
    {0x8b7c_5091_6fc9_99f4L, 0x2090_9cda_923b_af84L, 0},
    {0x8c9d_ce6f_7b8e_d1a6L, 0x9907_93fa_2ca5_3a1aL, 0},
    {0x9920_0000_0000_0000L, 0x08c5_0000_0000_0000L, 0},
    {0x9ae0_0000_0000_0000L, 0x0e9c_0000_0000_0000L, 0},
    {0xa192_6dee_3ae9_ff0aL, 0xba60_0000_0000_0000L, 0},
    {0xa23f_9f5f_02e2_ccd6L, 0x2f81_67c3_9503_829dL, 0},
    {0xcbfc_15ab_001e_11ccL, 0x0724_86c8_f96b_e096L, 0},
    {0xd880_0000_0000_0000L, 0xc6bd_8171_578f_2264L, 0},
    {0xf4e4_b9ff_f4ff_6bf3L, 0x3ccf_6b96_05fe_b0acL, 0},
    {0xfae7_3b5b_f373_db7fL, 0xa9ce_aee4_c5dc_851aL, 0}
  };

  private Bid64IntelVectorTest() {
  }

  public static void main(String[] args) {
    testClassVectors();
    testCanonicalVectors();
    testPredicateBoundaryVectors();
    testSameQuantumVectors();
    testTotalOrderVectors();
    System.out.println("Bid64IntelVectorTest: all tests passed");
  }

  private static void testClassVectors() {
    for (long[] vector : CLASS_VECTORS) {
      Bid64 value = Bid64.fromRawBits(vector[0]);
      int expected = (int) vector[1];
      int actual = value.classify().ordinal();
      if (expected != actual) {
        throw new IllegalStateException(
            String.format(
                "class(0x%016x): expected %d, actual %d (%s)",
                vector[0], expected, actual, value.classify()));
      }
    }

    equal(9, Bid64.POSITIVE_INFINITY.classify().ordinal(), "positive infinity");
    equal(2, Bid64.NEGATIVE_INFINITY.classify().ordinal(), "negative infinity");
    equal(1, Bid64.QUIET_NAN.classify().ordinal(), "quiet NaN");
    equal(0, Bid64.SIGNALING_NAN.classify().ordinal(), "signaling NaN");
  }

  private static void testCanonicalVectors() {
    for (long[] vector : CANONICAL_VECTORS) {
      boolean expected = vector[1] != 0;
      boolean actual = Bid64.fromRawBits(vector[0]).isCanonical();
      if (expected != actual) {
        throw new IllegalStateException(
            String.format(
                "isCanonical(0x%016x): expected %s, actual %s",
                vector[0], expected, actual));
      }
    }
  }

  private static void testPredicateBoundaryVectors() {
    check(Bid64.fromRawBits(0x0000_0100_0020_0040L).isSubnormal(), "subnormal");
    check(Bid64.fromRawBits(0x0042_0008_8006_0800L).isNormal(), "normal");
    check(!Bid64.fromRawBits(0x6eab_a7a5_b910_bc03L).isNormal(), "non-normal");

    check(!Bid64.fromRawBits(0x6023_86f2_6fc0_fffeL).isZero(), "largest canonical");
    check(!Bid64.fromRawBits(0x6023_86f2_6fc0_ffffL).isZero(), "canonical boundary");
    check(Bid64.fromRawBits(0x6023_86f2_6fc1_0000L).isZero(), "non-canonical boundary");
    check(Bid64.fromRawBits(0x6023_86f2_6fc1_ffffL).isZero(), "non-canonical");

    check(Bid64.fromRawBits(0x7e00_0001_0000_0000L).isSignalingNaN(), "sNaN");
    check(Bid64.fromRawBits(0xfe00_0000_0000_0000L).isSignalingNaN(), "negative sNaN");
  }

  private static void testSameQuantumVectors() {
    for (long[] vector : SAME_QUANTUM_VECTORS) {
      boolean expected = vector[2] != 0;
      boolean actual =
          Bid64.fromRawBits(vector[0]).sameQuantum(Bid64.fromRawBits(vector[1]));
      if (expected != actual) {
        throw new IllegalStateException(
            String.format(
                "sameQuantum(0x%016x, 0x%016x): expected %s, actual %s",
                vector[0], vector[1], expected, actual));
      }
    }

    check(Bid64.POSITIVE_ZERO.sameQuantum(Bid64.NEGATIVE_ZERO), "zero quantum");
    check(
        Bid64.POSITIVE_INFINITY.sameQuantum(Bid64.NEGATIVE_INFINITY),
        "infinity quantum");
    check(Bid64.QUIET_NAN.sameQuantum(Bid64.SIGNALING_NAN), "NaN quantum");
    check(!Bid64.QUIET_NAN.sameQuantum(Bid64.POSITIVE_INFINITY), "NaN vs infinity");
  }

  private static final long[][] TOTAL_ORDER_VECTORS = {
    {0x0000000000000000L, 0x0000000000000000L, 1},
    {0x0000000000000000L, 0x6aeb34ffd6033a6bL, 1},
    {0x0006082180080050L, 0x6dce1c55d77c6627L, 0},
    {0x0010230100040000L, 0x0000000000000000L, 0},
    {0x0c3b000000000000L, 0xbe33a88e4eb91a55L, 0},
    {0x3c7e3b50324cdad4L, 0x1f8869a841318bd3L, 0},
    {0x3f60e6ce2fb54e99L, 0x8560efb99fe1a25aL, 0},
    {0x4151292dc7ddfb1fL, 0x7bfffe6ecafbffffL, 1},
    {0x41849debe6a63955L, 0xdbffacb4c26fd3d7L, 0},
    {0x44b806eafb78769dL, 0x0640000000000000L, 0},
    {0x4b6d62a46e996446L, 0xe3c8c6341743c275L, 0},
    {0x66e2cfdfe3bfbfdfL, 0xe76a78fade4d5645L, 0},
    {0x751e6e94d0717b7eL, 0x2a1f1efd5d0ad7daL, 1},
    {0x7bfb891c1f1ffc2fL, 0x1af564ed868185c2L, 0},
    {0x7dab90d3d035a79aL, 0x7dfb854bdd6bbd27L, 1},
    {0x7eff8406185cfc33L, 0x7ff3dfdbb7dbccdfL, 0},
    {0x7f6cb6613f7fcf9fL, 0x7d4bbd7355a38753L, 1},
    {0x7ffdfdfffbeb6fffL, 0x7edb7be3d55e3ff6L, 1},
    {0x7ffffffedff99ffbL, 0xfddadb79ef5f5fffL, 0},
    {0x8532eede32e2a8cfL, 0x47080932e15557fbL, 1},
    {0x9be87b086d79f76aL, 0xfcf3f1f5adbde7efL, 0},
    {0xa2eb6743df3efd23L, 0x49dce550ac84506eL, 1},
    {0xa3267a003b7ab3deL, 0xba58fbd307145e22L, 0},
    {0xafa2e30e0bbf5cbcL, 0xa26ddb6b5f642f7eL, 1},
    {0xc301c82408095502L, 0xc1db421e000440e8L, 1},
    {0xcc2ae74b8f509bd5L, 0xcc84166baa1cff47L, 0},
    {0xd10505fc78a67979L, 0x9cc0000000000000L, 1},
    {0xd9b8810fefc917b0L, 0x44cd418c6d196c9fL, 1},
    {0xe0c377cb6a1bfd75L, 0xec0b6505adc3dd15L, 0},
    {0xe7f2d9d4efddf9edL, 0x9ecca8e8e2e6225bL, 1},
    {0xf4dd3a17d84856c2L, 0xefbed79af883dff7L, 1},
    {0xf7ffdfffefbf7fffL, 0xf7fffffffffaff7fL, 1},
    {0xf8f48edf7b7eed66L, 0x9828040808fe386dL, 1},
    {0xfdedcae7dddedc7bL, 0xfcf697bfb2f5be8dL, 0},
    {0xffbffd77ffdbbf6eL, 0xfffeafdbfffef7ffL, 0},
    {0xffbffffffeffdfffL, 0xffffffffffffffffL, 1},
    {0xffffeffbffefbfffL, 0xf4fe78fdf4e4e9d7L, 1},
    {0xffffffffffffffffL, 0xfd9c97fbff77fff7L, 0}
  };

  private static final long[][] TOTAL_ORDER_MAG_VECTORS = {
    {0x0000000000000000L, 0x0000000000000000L, 1},
    {0x0000000000000000L, 0xd20670083d534a46L, 1},
    {0x00483400205a0108L, 0xadd057004d852602L, 1},
    {0x00b38b8003045514L, 0x0200000200000010L, 1},
    {0x0805627795f7eba4L, 0xfff6fffffff7fffdL, 1},
    {0x0b00940c0b1b4608L, 0xfa41ef73c1309469L, 1},
    {0x0e805bff31e759b3L, 0x1ca72375d331f47eL, 1},
    {0x1042838468a08140L, 0x0000000000000000L, 0},
    {0x114c9d58a9875a64L, 0xb7ee522b503e48ecL, 1},
    {0x1ac0000000000000L, 0x0ae3000000000000L, 1},
    {0x41ea521fb46122c8L, 0xc680b612e2260824L, 1},
    {0x562c25bc1f510abeL, 0xf0db2d59a451555aL, 0},
    {0x6485f1b24ce8f132L, 0x46a4d9ff4d79acc6L, 1},
    {0x7de33f7afafffbe7L, 0xff3e7ecffaee79bbL, 0},
    {0x7ebfbeccd7abf5fdL, 0xf3a8462662a1b521L, 0},
    {0x7ff7f7d77ff3955dL, 0xffffcfbfffffffffL, 1},
    {0x7fff7fb3fff7fbffL, 0xffffbfdffffffffeL, 0},
    {0x8019705df2d97759L, 0x39f7cd4942b7e9bdL, 1},
    {0x861f7326409d7e2fL, 0x98b2ffef06945516L, 1},
    {0x9760a805c4aca603L, 0x690eb432257773e3L, 0},
    {0x9cff4f9fbb20d24cL, 0xc3904953ae1cae58L, 1},
    {0xae94a16fa3e37d1eL, 0xc7474bda93dac462L, 1},
    {0xb1c17d35c37c6403L, 0xb0de5d7824d03b0bL, 0},
    {0xd200000000000000L, 0xeec1895edf539d95L, 1},
    {0xdf8dafd6481a0b9bL, 0xf41933285dc060a0L, 0},
    {0xebb10b24d1cc0719L, 0xc22aa1ef3bb5db78L, 1},
    {0xebfffbffffffffffL, 0x6e97e7f87bc60295L, 1},
    {0xeff968927d3c49e4L, 0x923a2ad67a9ea634L, 0},
    {0xf81696b10870a09aL, 0x4de22fca6a2c3fa4L, 0},
    {0xff7b3ebefedc78fcL, 0xffb6ffff7f7ffff3L, 0},
    {0x7c00000000000000L, 0x7e00000000000000L, 0},
    {0x7e00000000000000L, 0x7c00000000000000L, 1},
    {0xfc00000000000000L, 0xfe00000000000000L, 0},
    {0xfe00000000000000L, 0xfc00000000000000L, 1},
    {0x31c0000000000001L, 0x2fe38d7ea4c68000L, 0},
    {0xb1c0000000000001L, 0xafe38d7ea4c68000L, 0},
    {0x7c03000000000001L, 0x7c03000000000002L, 1}
  };

  private static void testTotalOrderVectors() {
    for (long[] vector : TOTAL_ORDER_VECTORS) {
      boolean expected = vector[2] != 0;
      boolean actual =
          Bid64.fromRawBits(vector[0]).totalOrder(Bid64.fromRawBits(vector[1]));
      if (expected != actual) {
        throw new IllegalStateException(
            String.format(
                "totalOrder(0x%016x, 0x%016x): expected %s, actual %s",
                vector[0], vector[1], expected, actual));
      }
    }
    for (long[] vector : TOTAL_ORDER_MAG_VECTORS) {
      boolean expected = vector[2] != 0;
      boolean actual =
          Bid64.fromRawBits(vector[0]).totalOrderMag(Bid64.fromRawBits(vector[1]));
      if (expected != actual) {
        throw new IllegalStateException(
            String.format(
                "totalOrderMag(0x%016x, 0x%016x): expected %s, actual %s",
                vector[0], vector[1], expected, actual));
      }
    }
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }

  private static void equal(int expected, int actual, String message) {
    if (expected != actual) {
      throw new IllegalStateException(
          message + ": expected " + expected + ", actual " + actual);
    }
  }
}
