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

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Disabled
public class TestSparkBloomFilter {

    // the implemented fpp limit is only approximating the hard boundary,
    // so we'll need an error threshold for the assertion
    final double FPP_ERROR_FACTOR = 0.05;

    final long ONE_GB = 1024L * 1024L * 1024L;

    private static Instant START;
    private Instant start;

    @BeforeAll
    public static void beforeAll() {
        START = Instant.now();
    }

    @AfterAll
    public static void afterAll() {
        Duration duration = Duration.between(START, Instant.now());
        System.err.println(duration + " TOTAL");
    }

    @BeforeEach
    public void beforeEach() {
        start = Instant.now();
    }

    @AfterEach
    public void afterEach(TestInfo testInfo) {
        Duration duration = Duration.between(start, Instant.now());
        System.err.println(duration + " " + testInfo.getDisplayName());
    }

    @CartesianTest
    public void testAccuracy(
      @Values(longs = {1_000_000L, 1_000_000_000L, 5_000_000_000L}) long numItems,
      @Values(doubles = {0.05, 0.03, 0.01, 0.001}) double expectedFpp
    ) {
        long optimalNumOfBytes = BloomFilter.optimalNumOfBits(numItems, expectedFpp) / Byte.SIZE;
        System.err.printf("bitArray: %d MB", optimalNumOfBytes / 1024 / 1024);
        Assumptions.assumeTrue(
            optimalNumOfBytes < 4 * ONE_GB,
            "this testcase would require allocating more than 4GB of heap mem (" + optimalNumOfBytes + ")"
        );

        //

        BloomFilter bloomFilter = BloomFilter.create(numItems, expectedFpp);

        for (long i = 0; i < numItems; i++) {
            if (i % 10_000_000 == 0) {
                System.err.printf(
                    "i: %d, bitCount: %d, b/i: %f, size: %d\n",
                    i,
                    bloomFilter.cardinality(),
                    (double) bloomFilter.cardinality() / i,
                    bloomFilter.bitSize()
                );
            }
            bloomFilter.putLong(2 * i);
        }

        long mightContainEven = 0;
        long mightContainOdd = 0;

        for (long i = 0; i < numItems; i++) {
            if (i % 10_000_000 == 0) {
                System.err.printf("i: %d\n", i);
            }

            long even = 2 * i;
            if (bloomFilter.mightContainLong(even)) {
                mightContainEven++;
            }

            long odd = 2 * i + 1;
            if (bloomFilter.mightContainLong(odd)) {
                mightContainOdd++;
            }
        }

        Assertions.assertEquals(
                numItems, mightContainEven,
                "mightContainLong must return true for all inserted numbers"
        );

        double actualFpp = (double) mightContainOdd / numItems;
        double acceptableFpp = expectedFpp * (1 + FPP_ERROR_FACTOR);

        System.err.printf("expectedFpp:   %f %%\n", 100 * expectedFpp);
        System.err.printf("acceptableFpp: %f %%\n", 100 * acceptableFpp);
        System.err.printf("actualFpp:     %f %%\n", 100 * actualFpp);

        Assumptions.assumeTrue(
                actualFpp <= acceptableFpp,
                String.format(
                  "acceptableFpp(%f %%) < actualFpp (%f %%)",
                  100 * acceptableFpp,
                  100 * actualFpp
                )
        );

        Assertions.assertTrue(
                actualFpp <= acceptableFpp,
                String.format(
                        "acceptableFpp(%f %%) < actualFpp (%f %%)",
                        100 * acceptableFpp,
                        100 * actualFpp
                )
        );
    }
}
