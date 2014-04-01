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

import java.util.Set;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonAnySetter;

/**
 * A {@link LoggedSingleRelativeRanking} represents an X-Y coordinate of a
 * single point in a discrete CDF.
 * 
 * All of the public methods are simply accessors for the instance variables we
 * want to write out in the JSON files.
 * 
 */
public class LoggedSingleRelativeRanking implements DeepCompare {
  /**
   * The Y coordinate, as a fraction {@code ( 0.0D, 1.0D )}. The default value
   * is there to mark an unfilled-in value.
   */
  double relativeRanking = -1.0D;
  /**
   * The X coordinate
   */
  long datum = -1L;

  static private Set<String> alreadySeenAnySetterAttributes =
      new TreeSet<String>();

  @SuppressWarnings("unused")
  // for input parameter ignored.
  @JsonAnySetter
  public void setUnknownAttribute(String attributeName, Object ignored) {
    if (!alreadySeenAnySetterAttributes.contains(attributeName)) {
      alreadySeenAnySetterAttributes.add(attributeName);
      System.err.println("In LoggedJob, we saw the unknown attribute "
          + attributeName + ".");
    }
  }

  public double getRelativeRanking() {
    return relativeRanking;
  }

  void setRelativeRanking(double relativeRanking) {
    this.relativeRanking = relativeRanking;
  }

  public long getDatum() {
    return datum;
  }

  void setDatum(long datum) {
    this.datum = datum;
  }

  private void compare1(long c1, long c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  private void compare1(double c1, double c2, TreePath loc, String eltname)
      throws DeepInequalityException {
    if (c1 != c2) {
      throw new DeepInequalityException(eltname + " miscompared", new TreePath(
          loc, eltname));
    }
  }

  public void deepCompare(DeepCompare comparand, TreePath loc)
      throws DeepInequalityException {
    if (!(comparand instanceof LoggedSingleRelativeRanking)) {
      throw new DeepInequalityException("comparand has wrong type", loc);
    }

    LoggedSingleRelativeRanking other = (LoggedSingleRelativeRanking) comparand;

    compare1(relativeRanking, other.relativeRanking, loc, "relativeRanking");
    compare1(datum, other.datum, loc, "datum");
  }
}
