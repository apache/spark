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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Implement DBSplitter over integer values.
 */
public class IntegerSplitter implements DBSplitter {
  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    long minVal = results.getLong(1);
    long maxVal = results.getLong(2);

    String lowClausePrefix = colName + " >= ";
    String highClausePrefix = colName + " < ";

    int numSplits = conf.getInt("mapred.map.tasks", 1);
    if (numSplits < 1) {
      numSplits = 1;
    }

    if (results.getString(1) == null && results.getString(2) == null) {
      // Range is null to null. Return a null split accordingly.
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    // Get all the split points together.
    List<Long> splitPoints = split(numSplits, minVal, maxVal);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // Turn the split points into a set of intervals.
    long start = splitPoints.get(0);
    for (int i = 1; i < splitPoints.size(); i++) {
      long end = splitPoints.get(i);

      if (i == splitPoints.size() - 1) {
        // This is the last one; use a closed interval.
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + Long.toString(start),
            colName + " <= " + Long.toString(end)));
      } else {
        // Normal open-interval case.
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + Long.toString(start),
            highClausePrefix + Long.toString(end)));
      }

      start = end;
    }

    if (results.getString(1) == null || results.getString(2) == null) {
      // At least one extrema is null; add a null split.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
    }

    return splits;
  }

  /**
   * Returns a list of longs one element longer than the list of input splits.
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
  List<Long> split(long numSplits, long minVal, long maxVal)
      throws SQLException {

    List<Long> splits = new ArrayList<Long>();

    // Use numSplits as a hint. May need an extra task if the size doesn't
    // divide cleanly.

    long splitSize = (maxVal - minVal) / numSplits;
    if (splitSize < 1) {
      splitSize = 1;
    }

    long curVal = minVal;

    while (curVal <= maxVal) {
      splits.add(curVal);
      curVal += splitSize;
    }

    if (splits.get(splits.size() - 1) != maxVal || splits.size() == 1) {
      // We didn't end on the maxVal. Add that to the end of the list.
      splits.add(maxVal);
    }

    return splits;
  }
}
