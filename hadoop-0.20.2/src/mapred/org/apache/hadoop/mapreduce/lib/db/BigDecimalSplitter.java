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

package org.apache.hadoop.mapreduce.lib.db;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Implement DBSplitter over BigDecimal values.
 */
public class BigDecimalSplitter implements DBSplitter {
  private static final Log LOG = LogFactory.getLog(BigDecimalSplitter.class);

  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    BigDecimal minVal = results.getBigDecimal(1);
    BigDecimal maxVal = results.getBigDecimal(2);

    String lowClausePrefix = colName + " >= ";
    String highClausePrefix = colName + " < ";

    BigDecimal numSplits = new BigDecimal(conf.getInt("mapred.map.tasks", 1));

    if (minVal == null && maxVal == null) {
      // Range is null to null. Return a null split accordingly.
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    if (minVal == null || maxVal == null) {
      // Don't know what is a reasonable min/max value for interpolation. Fail.
      LOG.error("Cannot find a range for NUMERIC or DECIMAL fields with one end NULL.");
      return null;
    }

    // Get all the split points together.
    List<BigDecimal> splitPoints = split(numSplits, minVal, maxVal);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // Turn the split points into a set of intervals.
    BigDecimal start = splitPoints.get(0);
    for (int i = 1; i < splitPoints.size(); i++) {
      BigDecimal end = splitPoints.get(i);

      if (i == splitPoints.size() - 1) {
        // This is the last one; use a closed interval.
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + start.toString(),
            colName + " <= " + end.toString()));
      } else {
        // Normal open-interval case.
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + start.toString(),
            highClausePrefix + end.toString()));
      }

      start = end;
    }

    return splits;
  }

  private static final BigDecimal MIN_INCREMENT = new BigDecimal(10000 * Double.MIN_VALUE);

  /**
   * Divide numerator by denominator. If impossible in exact mode, use rounding.
   */
  protected BigDecimal tryDivide(BigDecimal numerator, BigDecimal denominator) {
    try {
      return numerator.divide(denominator);
    } catch (ArithmeticException ae) {
      return numerator.divide(denominator, BigDecimal.ROUND_HALF_UP);
    }
  }

  /**
   * Returns a list of BigDecimals one element longer than the list of input splits.
   * This represents the boundaries between input splits.
   * All splits are open on the top end, except the last one.
   *
   * So the list [0, 5, 8, 12, 18] would represent splits capturing the intervals:
   *
   * [0, 5)
   * [5, 8)
   * [8, 12)
   * [12, 18] note the closed interval for the last split.
   */
  List<BigDecimal> split(BigDecimal numSplits, BigDecimal minVal, BigDecimal maxVal)
      throws SQLException {

    List<BigDecimal> splits = new ArrayList<BigDecimal>();

    // Use numSplits as a hint. May need an extra task if the size doesn't
    // divide cleanly.

    BigDecimal splitSize = tryDivide(maxVal.subtract(minVal), (numSplits));
    if (splitSize.compareTo(MIN_INCREMENT) < 0) {
      splitSize = MIN_INCREMENT;
      LOG.warn("Set BigDecimal splitSize to MIN_INCREMENT");
    }

    BigDecimal curVal = minVal;

    while (curVal.compareTo(maxVal) <= 0) {
      splits.add(curVal);
      curVal = curVal.add(splitSize);
    }

    if (splits.get(splits.size() - 1).compareTo(maxVal) != 0 || splits.size() == 1) {
      // We didn't end on the maxVal. Add that to the end of the list.
      splits.add(maxVal);
    }

    return splits;
  }
}
