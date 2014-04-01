/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

/**
 * A class that generates random numbers that follow some distribution.
 */
public class RandomDistribution {
  /**
   * Interface for discrete (integer) random distributions.
   */
  public static interface DiscreteRNG {
    /**
     * Get the next random number
     * 
     * @return the next random number.
     */
    public int nextInt();
  }

  /**
   * P(i)=1/(max-min)
   */
  public static final class Flat implements DiscreteRNG {
    private final Random random;
    private final int min;
    private final int max;

    /**
     * Generate random integers from min (inclusive) to max (exclusive)
     * following even distribution.
     * 
     * @param random
     *          The basic random number generator.
     * @param min
     *          Minimum integer
     * @param max
     *          maximum integer (exclusive).
     * 
     */
    public Flat(Random random, int min, int max) {
      if (min >= max) {
        throw new IllegalArgumentException("Invalid range");
      }
      this.random = random;
      this.min = min;
      this.max = max;
    }
    
    /**
     * @see DiscreteRNG#nextInt()
     */
    @Override
    public int nextInt() {
      return random.nextInt(max - min) + min;
    }
  }

  /**
   * Zipf distribution. The ratio of the probabilities of integer i and j is
   * defined as follows:
   * 
   * P(i)/P(j)=((j-min+1)/(i-min+1))^sigma.
   */
  public static final class Zipf implements DiscreteRNG {
    private static final double DEFAULT_EPSILON = 0.001;
    private final Random random;
    private final ArrayList<Integer> k;
    private final ArrayList<Double> v;

    /**
     * Constructor
     * 
     * @param r
     *          The random number generator.
     * @param min
     *          minimum integer (inclusvie)
     * @param max
     *          maximum integer (exclusive)
     * @param sigma
     *          parameter sigma. (sigma > 1.0)
     */
    public Zipf(Random r, int min, int max, double sigma) {
      this(r, min, max, sigma, DEFAULT_EPSILON);
    }

    /**
     * Constructor.
     * 
     * @param r
     *          The random number generator.
     * @param min
     *          minimum integer (inclusvie)
     * @param max
     *          maximum integer (exclusive)
     * @param sigma
     *          parameter sigma. (sigma > 1.0)
     * @param epsilon
     *          Allowable error percentage (0 < epsilon < 1.0).
     */
    public Zipf(Random r, int min, int max, double sigma, double epsilon) {
      if ((max <= min) || (sigma <= 1) || (epsilon <= 0)
          || (epsilon >= 0.5)) {
        throw new IllegalArgumentException("Invalid arguments");
      }
      random = r;
      k = new ArrayList<Integer>();
      v = new ArrayList<Double>();

      double sum = 0;
      int last = -1;
      for (int i = min; i < max; ++i) {
        sum += Math.exp(-sigma * Math.log(i - min + 1));
        if ((last == -1) || i * (1 - epsilon) > last) {
          k.add(i);
          v.add(sum);
          last = i;
        }
      }

      if (last != max - 1) {
        k.add(max - 1);
        v.add(sum);
      }

      v.set(v.size() - 1, 1.0);

      for (int i = v.size() - 2; i >= 0; --i) {
        v.set(i, v.get(i) / sum);
      }
    }

    /**
     * @see DiscreteRNG#nextInt()
     */
    @Override
    public int nextInt() {
      double d = random.nextDouble();
      int idx = Collections.binarySearch(v, d);

      if (idx > 0) {
        ++idx;
      }
      else {
        idx = -(idx + 1);
      }

      if (idx >= v.size()) {
        idx = v.size() - 1;
      }

      if (idx == 0) {
        return k.get(0);
      }

      int ceiling = k.get(idx);
      int lower = k.get(idx - 1);

      return ceiling - random.nextInt(ceiling - lower);
    }
  }

  /**
   * Binomial distribution.
   * 
   * P(k)=select(n, k)*p^k*(1-p)^(n-k) (k = 0, 1, ..., n)
   * 
   * P(k)=select(max-min-1, k-min)*p^(k-min)*(1-p)^(k-min)*(1-p)^(max-k-1)
   */
  public static final class Binomial implements DiscreteRNG {
    private final Random random;
    private final int min;
    private final int n;
    private final double[] v;

    private static double select(int n, int k) {
      double ret = 1.0;
      for (int i = k + 1; i <= n; ++i) {
        ret *= (double) i / (i - k);
      }
      return ret;
    }
    
    private static double power(double p, int k) {
      return Math.exp(k * Math.log(p));
    }

    /**
     * Generate random integers from min (inclusive) to max (exclusive)
     * following Binomial distribution.
     * 
     * @param random
     *          The basic random number generator.
     * @param min
     *          Minimum integer
     * @param max
     *          maximum integer (exclusive).
     * @param p
     *          parameter.
     * 
     */
    public Binomial(Random random, int min, int max, double p) {
      if (min >= max) {
        throw new IllegalArgumentException("Invalid range");
      }
      this.random = random;
      this.min = min;
      this.n = max - min - 1;
      if (n > 0) {
        v = new double[n + 1];
        double sum = 0.0;
        for (int i = 0; i <= n; ++i) {
          sum += select(n, i) * power(p, i) * power(1 - p, n - i);
          v[i] = sum;
        }
        for (int i = 0; i <= n; ++i) {
          v[i] /= sum;
        }
      }
      else {
        v = null;
      }
    }

    /**
     * @see DiscreteRNG#nextInt()
     */
    @Override
    public int nextInt() {
      if (v == null) {
        return min;
      }
      double d = random.nextDouble();
      int idx = Arrays.binarySearch(v, d);
      if (idx > 0) {
        ++idx;
      } else {
        idx = -(idx + 1);
      }

      if (idx >= v.length) {
        idx = v.length - 1;
      }
      return idx + min;
    }
  }
}
