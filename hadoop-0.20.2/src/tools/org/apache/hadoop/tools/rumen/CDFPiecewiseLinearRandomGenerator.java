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
 */
package org.apache.hadoop.tools.rumen;

public class CDFPiecewiseLinearRandomGenerator extends CDFRandomGenerator {

  /**
   * @param cdf
   *          builds a CDFRandomValue engine around this
   *          {@link LoggedDiscreteCDF}, with a defaultly seeded RNG
   */
  public CDFPiecewiseLinearRandomGenerator(LoggedDiscreteCDF cdf) {
    super(cdf);
  }

  /**
   * @param cdf
   *          builds a CDFRandomValue engine around this
   *          {@link LoggedDiscreteCDF}, with an explicitly seeded RNG
   * @param seed
   *          the random number generator seed
   */
  public CDFPiecewiseLinearRandomGenerator(LoggedDiscreteCDF cdf, long seed) {
    super(cdf, seed);
  }

  /**
   * TODO This code assumes that the empirical minimum resp. maximum is the
   * epistomological minimum resp. maximum. This is probably okay for the
   * minimum, because that likely represents a task where everything went well,
   * but for the maximum we may want to develop a way of extrapolating past the
   * maximum.
   */
  @Override
  public long valueAt(double probability) {
    int rangeFloor = floorIndex(probability);

    double segmentProbMin = getRankingAt(rangeFloor);
    double segmentProbMax = getRankingAt(rangeFloor + 1);

    long segmentMinValue = getDatumAt(rangeFloor);
    long segmentMaxValue = getDatumAt(rangeFloor + 1);

    // If this is zero, this object is based on an ill-formed cdf
    double segmentProbRange = segmentProbMax - segmentProbMin;
    long segmentDatumRange = segmentMaxValue - segmentMinValue;

    long result = (long) ((probability - segmentProbMin) / segmentProbRange * segmentDatumRange)
        + segmentMinValue;

    return result;
  }
}
