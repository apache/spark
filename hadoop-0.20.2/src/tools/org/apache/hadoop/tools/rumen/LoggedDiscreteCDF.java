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

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link LoggedDiscreteCDF} is a discrete approximation of a cumulative
 * distribution function, with this class set up to meet the requirements of the
 * Jackson JSON parser/generator.
 * 
 * All of the public methods are simply accessors for the instance variables we
 * want to write out in the JSON files.
 * 
 */
public class LoggedDiscreteCDF implements DeepCompare {
  /**
   * The number of values this CDF is built on
   */
  long numberValues = -1L;
  /**
   * The least {@code X} value
   */
  long minimum = Long.MIN_VALUE;
  /**
   * The coordinates of the bulk of the CDF
   */
  List<LoggedSingleRelativeRanking> rankings = new ArrayList<LoggedSingleRelativeRanking>();
  /**
   * The greatest {@code X} value
   */
  long maximum = Long.MAX_VALUE;

  void setCDF(Histogram data, int[] steps, int modulus) {

    numberValues = data.getTotalCount();
    long[] CDF = data.getCDF(modulus, steps);

    if (CDF != null) {
      minimum = CDF[0];
      maximum = CDF[CDF.length - 1];

      rankings = new ArrayList<LoggedSingleRelativeRanking>();

      for (int i = 1; i < CDF.length - 1; ++i) {
        LoggedSingleRelativeRanking srr = new LoggedSingleRelativeRanking();

        srr.setRelativeRanking(((double) steps[i - 1]) / modulus);
        srr.setDatum(CDF[i]);

        rankings.add(srr);
      }
    }
  }

  public long getMinimum() {
    return minimum;
  }

  void setMinimum(long minimum) {
    this.minimum = minimum;
  }

  public List<LoggedSingleRelativeRanking> getRankings() {
    return rankings;
  }

  void setRankings(List<LoggedSingleRelativeRanking> rankings) {
    this.rankings = rankings;
  }

  public long getMaximum() {
    return maximum;
  }

  void setMaximum(long maximum) {
    this.maximum = maximum;
  }

  public long getNumberValues() {
    return numberValues;
  }

  void setNumberValues(long numberValues) {
    this.numberValues = numberValues;
  }

  private void compare1(long c1, long c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(List<LoggedSingleRelativeRanking> c1,
      List<LoggedSingleRelativeRanking> c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 == null && c2 == null) {
      return;
    }

    if (c1 == null || c2 == null || c1.size() != c2.size()) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }

    for (int i = 0; i < c1.size(); ++i) {
      c1.get(i).deepCompare(c2.get(i), new TreePath(loc, eltname, i));
    }
  }

  public void deepCompare(DeepCompare comparand, TreePath loc)
      throws DeepInequalityException {
    if (!(comparand instanceof LoggedDiscreteCDF)) {
      throw new DeepInequalityException("comparand has wrong type", loc);
    }

    LoggedDiscreteCDF other = (LoggedDiscreteCDF) comparand;

    compare1(numberValues, other.numberValues, loc, "numberValues");

    compare1(minimum, other.minimum, loc, "minimum");
    compare1(maximum, other.maximum, loc, "maximum");

    compare1(rankings, other.rankings, loc, "rankings");
  }
}
