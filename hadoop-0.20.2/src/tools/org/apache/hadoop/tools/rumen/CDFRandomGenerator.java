/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package org.apache.hadoop.tools.rumen;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * An instance of this class generates random values that confirm to the
 * embedded {@link LoggedDiscreteCDF} . The discrete CDF is a pointwise
 * approximation of the "real" CDF. We therefore have a choice of interpolation
 * rules.
 * 
 * A concrete subclass of this abstract class will implement valueAt(double)
 * using a class-dependent interpolation rule.
 * 
 */
public abstract class CDFRandomGenerator {
  final double[] rankings;
  final long[] values;

  final Random random;

  CDFRandomGenerator(LoggedDiscreteCDF cdf) {
    this(cdf, new Random());
  }

  CDFRandomGenerator(LoggedDiscreteCDF cdf, long seed) {
    this(cdf, new Random(seed));
  }

  private CDFRandomGenerator(LoggedDiscreteCDF cdf, Random random) {
    this.random = random;
    rankings = new double[cdf.getRankings().size() + 2];
    values = new long[cdf.getRankings().size() + 2];
    initializeTables(cdf);
  }

  protected final void initializeTables(LoggedDiscreteCDF cdf) {
    rankings[0] = 0.0;
    values[0] = cdf.getMinimum();
    rankings[rankings.length - 1] = 1.0;
    values[rankings.length - 1] = cdf.getMaximum();

    List<LoggedSingleRelativeRanking> subjects = cdf.getRankings();

    for (int i = 0; i < subjects.size(); ++i) {
      rankings[i + 1] = subjects.get(i).getRelativeRanking();
      values[i + 1] = subjects.get(i).getDatum();
    }
  }

  protected int floorIndex(double probe) {
    int result = Arrays.binarySearch(rankings, probe);

    return Math.abs(result + 1) - 1;
  }

  protected double getRankingAt(int index) {
    return rankings[index];
  }

  protected long getDatumAt(int index) {
    return values[index];
  }

  public long randomValue() {
    return valueAt(random.nextDouble());
  }

  public abstract long valueAt(double probability);
}
