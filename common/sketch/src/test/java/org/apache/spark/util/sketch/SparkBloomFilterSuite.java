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
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;


@Tag("BloomFilter")
@Tag("org.apache.spark.tags.SlowTest")
public class SparkBloomFilterSuite {

    // the implemented fpp limit is only approximating the hard boundary,
    // so we'll need an error threshold for the assertion
    final double FPP_EVEN_ODD_ERROR_FACTOR = 0.05;
    final double FPP_RANDOM_ERROR_FACTOR = 0.04;

    final long ONE_GB = 1024L * 1024L * 1024L;
    final long REQUIRED_HEAP_UPPER_BOUND_IN_BYTES = 4 * ONE_GB;

    private static Instant START;

    private Instant start;
    private final Map<String,PrintStream> testOutMap = new ConcurrentHashMap<>();

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
    public void beforeEach(
            TestInfo testInfo
    ) throws Exception {
        start = Instant.now();

        String testName = testInfo.getDisplayName();

        String testClassName = getClass().getName();
        String logDir = System.getProperty(testClassName+ ".logDir", "./target/tmp");
        Path logDirPath = Path.of(logDir);
        Files.createDirectories(logDirPath);
        Path testLogPath = Path.of(logDir,testName + ".log");
        Files.deleteIfExists(testLogPath);

        PrintStream testOut = new PrintStream(Files.newOutputStream(testLogPath));
        testOutMap.put(testName, testOut);

        testOut.println("testName: " + testName);
    }

    @AfterEach
    public void afterEach(TestInfo testInfo) {
        Duration duration = Duration.between(start, Instant.now());

        String testName = testInfo.getDisplayName();
        PrintStream testOut = testOutMap.get(testName);

        testOut.println("duration: " + duration );
        testOut.close();
    }

    /**
     * This test, in N number of iterations, inserts N even numbers (2*i) int,
     * and leaves out N odd numbers (2*i+1) from the tested BloomFilter instance.
     *
     * It checks the 100% accuracy of mightContain=true on all of the even items,
     * and measures the mightContain=true (false positive) rate on the not-inserted odd numbers.
     *
     * @param numItems the number of items to be inserted
     * @param expectedFpp the expected fpp rate of the tested BloomFilter instance
     * @param deterministicSeed the deterministic seed to use to initialize
     *                          the primary BloomFilter instance.
     */
    @CartesianTest(name = "testAccuracyEvenOdd_{index}.n{0}_fpp{1}_seed{2}")
    public void testAccuracyEvenOdd(
      @Values(longs = {1_000_000L, 1_000_000_000L, 5_000_000_000L}) long numItems,
      @Values(doubles = {0.05, 0.03, 0.01, 0.001}) double expectedFpp,
      @Values(ints = {BloomFilterImpl.DEFAULT_SEED, 1, 127}) int deterministicSeed,
      TestInfo testInfo
    ) {
        String testName = testInfo.getDisplayName();
        PrintStream testOut = testOutMap.get(testName);

        long optimalNumOfBits = BloomFilter.optimalNumOfBits(numItems, expectedFpp);
        testOut.printf(
                "optimal   bitArray: %d (%d MB)\n",
                optimalNumOfBits,
                optimalNumOfBits / Byte.SIZE / 1024 / 1024
        );
        Assumptions.assumeTrue(
            optimalNumOfBits / Byte.SIZE < REQUIRED_HEAP_UPPER_BOUND_IN_BYTES,
            "this testcase would require allocating more than 4GB of heap mem ("
            + optimalNumOfBits
            + " bits)"
        );

        BloomFilter bloomFilter = BloomFilter.create(numItems, optimalNumOfBits, deterministicSeed);
        testOut.printf(
                "allocated bitArray: %d (%d MB)\n",
                bloomFilter.bitSize(),
                bloomFilter.bitSize() / Byte.SIZE / 1024 / 1024
        );

        for (long i = 0; i < numItems; i++) {
            if (i % 10_000_000 == 0) {
                System.err.printf("i: %d\n", i);
            }

            bloomFilter.putLong(2 * i);
        }

        testOut.printf("bitCount: %d\nsaturation: %f\n",
                bloomFilter.cardinality(),
                (double) bloomFilter.cardinality() / bloomFilter.bitSize()
        );

        long mightContainEven = 0;
        long mightContainOdd = 0;

        for (long i = 0; i < numItems; i++) {
            if (i % (numItems / 100) == 0) {
                System.err.printf("%s: %2d %%\n", testName, 100 * i / numItems);
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
        double acceptableFpp = expectedFpp * (1 + FPP_EVEN_ODD_ERROR_FACTOR);

        testOut.printf("expectedFpp:   %f %%\n", 100 * expectedFpp);
        testOut.printf("acceptableFpp: %f %%\n", 100 * acceptableFpp);
        testOut.printf("actualFpp:     %f %%\n", 100 * actualFpp);

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

    /**
     * This test inserts N pseudorandomly generated numbers in 2N number of iterations in two
     * differently seeded (theoretically independent) BloomFilter instances. All the random
     * numbers generated in an even-iteration will be inserted into both filters, all the
     * random numbers generated in an odd-iteration will be left out from both.
     *
     * The test checks the 100% accuracy of 'mightContain=true' for all the items inserted
     * in an even-loop. It counts the false positives as the number of odd-loop items for
     * which the primary filter reports 'mightContain=true', but secondary reports
     * 'mightContain=false'. Since we inserted the same elements into both instances,
     * and the secondary reports non-insertion, the 'mightContain=true' from the primary
     * can only be a false positive.
     *
     * @param numItems the number of items to be inserted
     * @param expectedFpp the expected fpp rate of the tested BloomFilter instance
     * @param deterministicSeed the deterministic seed to use to initialize
     *                          the primary BloomFilter instance. (The secondary will be
     *                          initialized with the constant seed of 0xCAFEBABE)
     */
    @Disabled
    @CartesianTest(name = "testAccuracyRandom_{index}.n{0}_fpp{1}_seed{2}")
    public void testAccuracyRandom(
            @Values(longs = {1_000_000L, 1_000_000_000L}) long numItems,
            @Values(doubles = {0.05, 0.03, 0.01, 0.001}) double expectedFpp,
            @Values(ints = {BloomFilterImpl.DEFAULT_SEED, 1, 127}) int deterministicSeed,
            TestInfo testInfo
    ) {
        String testName = testInfo.getDisplayName();
        PrintStream testOut = testOutMap.get(testName);

        long optimalNumOfBits = BloomFilter.optimalNumOfBits(numItems, expectedFpp);
        testOut.printf(
                "optimal   bitArray: %d (%d MB)\n",
                optimalNumOfBits,
                optimalNumOfBits / Byte.SIZE / 1024 / 1024
        );
        Assumptions.assumeTrue(
            2 * optimalNumOfBits / Byte.SIZE < REQUIRED_HEAP_UPPER_BOUND_IN_BYTES,
            "this testcase would require allocating more than 4GB of heap mem (2x "
            + optimalNumOfBits
            + " bits)"
        );

        BloomFilter bloomFilterPrimary =
                BloomFilter.create(numItems, optimalNumOfBits, deterministicSeed);
        BloomFilter bloomFilterSecondary =
                BloomFilter.create(numItems, optimalNumOfBits, 0xCAFEBABE);

        testOut.printf(
                "allocated bitArray: %d (%d MB)\n",
                bloomFilterPrimary.bitSize(),
                bloomFilterPrimary.bitSize() / Byte.SIZE / 1024 / 1024
        );


        Random pseudoRandom = new Random();
        long iterationCount = 2 * numItems;

        pseudoRandom.setSeed(deterministicSeed);
        for (long i = 0; i < iterationCount; i++) {
            if (i % 10_000_000 == 0) {
                System.err.printf("i: %d\n", i);
            }

            long candidate = pseudoRandom.nextLong();
            if (i % 2 == 0) {
                bloomFilterPrimary.putLong(candidate);
                bloomFilterSecondary.putLong(candidate);
            }
        }
        testOut.printf("bitCount: %d\nsaturation: %f\n",
                bloomFilterPrimary.cardinality(),
                (double) bloomFilterPrimary.cardinality() / bloomFilterPrimary.bitSize()
        );

        long mightContainEvenIndexed = 0;
        long mightContainOddIndexed = 0;
        long confirmedAsNotInserted = 0;

        pseudoRandom.setSeed(deterministicSeed);
        for (long i = 0; i < iterationCount; i++) {
            if (i % (iterationCount / 100) == 0) {
                System.err.printf("%s: %2d %%\n", testName, 100 * i / iterationCount);
            }

            long candidate = pseudoRandom.nextLong();

            if (i % 2 == 0) { // EVEN
                mightContainEvenIndexed++;
            } else { // ODD
                // for fpp estimation, only consider the odd indexes
                // (to avoid querying the secondary with elements known to be inserted)

                // since here we avoided all the even indexes,
                // most of these secondary queries will return false
                if (!bloomFilterSecondary.mightContainLong(candidate)) {
                    // from the odd indexes, we consider only those items
                    // where the secondary confirms the non-insertion

                    // anything on which the primary and the secondary
                    // disagrees here is a false positive
                    if (bloomFilterPrimary.mightContainLong(candidate)) {
                        mightContainOddIndexed++;
                    }
                    // count the total number of considered items for a baseline
                    confirmedAsNotInserted++;
                }
            }
        }

        Assertions.assertEquals(
                numItems, mightContainEvenIndexed,
                "mightContainLong must return true for all inserted numbers"
        );

        double actualFpp = (double) mightContainOddIndexed / confirmedAsNotInserted;
        double acceptableFpp = expectedFpp * (1 + FPP_RANDOM_ERROR_FACTOR);

        testOut.printf("mightContainOddIndexed: %10d\n", mightContainOddIndexed);
        testOut.printf("confirmedAsNotInserted: %10d\n", confirmedAsNotInserted);
        testOut.printf("numItems:               %10d\n", numItems);
        testOut.printf("expectedFpp:   %f %%\n", 100 * expectedFpp);
        testOut.printf("acceptableFpp: %f %%\n", 100 * acceptableFpp);
        testOut.printf("actualFpp:     %f %%\n", 100 * actualFpp);

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
