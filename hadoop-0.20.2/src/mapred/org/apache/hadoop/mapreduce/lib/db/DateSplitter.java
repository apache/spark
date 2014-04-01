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
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Implement DBSplitter over date/time values.
 * Make use of logic from IntegerSplitter, since date/time are just longs
 * in Java.
 */
public class DateSplitter extends IntegerSplitter {

  private static final Log LOG = LogFactory.getLog(DateSplitter.class);

  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    long minVal;
    long maxVal;

    int sqlDataType = results.getMetaData().getColumnType(1);
    minVal = resultSetColToLong(results, 1, sqlDataType);
    maxVal = resultSetColToLong(results, 2, sqlDataType);

    String lowClausePrefix = colName + " >= ";
    String highClausePrefix = colName + " < ";

    int numSplits = conf.getInt("mapred.map.tasks", 1);
    if (numSplits < 1) {
      numSplits = 1;
    }

    if (minVal == Long.MIN_VALUE && maxVal == Long.MIN_VALUE) {
      // The range of acceptable dates is NULL to NULL. Just create a single split.
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    // Gather the split point integers
    List<Long> splitPoints = split(numSplits, minVal, maxVal);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // Turn the split points into a set of intervals.
    long start = splitPoints.get(0);
    Date startDate = longToDate(start, sqlDataType);
    if (sqlDataType == Types.TIMESTAMP) {
      // The lower bound's nanos value needs to match the actual lower-bound nanos.
      try {
        ((java.sql.Timestamp) startDate).setNanos(results.getTimestamp(1).getNanos());
      } catch (NullPointerException npe) {
        // If the lower bound was NULL, we'll get an NPE; just ignore it and don't set nanos.
      }
    }

    for (int i = 1; i < splitPoints.size(); i++) {
      long end = splitPoints.get(i);
      Date endDate = longToDate(end, sqlDataType);

      if (i == splitPoints.size() - 1) {
        if (sqlDataType == Types.TIMESTAMP) {
          // The upper bound's nanos value needs to match the actual upper-bound nanos.
          try {
            ((java.sql.Timestamp) endDate).setNanos(results.getTimestamp(2).getNanos());
          } catch (NullPointerException npe) {
            // If the upper bound was NULL, we'll get an NPE; just ignore it and don't set nanos.
          }
        }
        // This is the last one; use a closed interval.
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + dateToString(startDate),
            colName + " <= " + dateToString(endDate)));
      } else {
        // Normal open-interval case.
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + dateToString(startDate),
            highClausePrefix + dateToString(endDate)));
      }

      start = end;
      startDate = endDate;
    }

    if (minVal == Long.MIN_VALUE || maxVal == Long.MIN_VALUE) {
      // Add an extra split to handle the null case that we saw.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
    }

    return splits;
  }

  /** Retrieve the value from the column in a type-appropriate manner and return
      its timestamp since the epoch. If the column is null, then return Long.MIN_VALUE.
      This will cause a special split to be generated for the NULL case, but may also
      cause poorly-balanced splits if most of the actual dates are positive time
      since the epoch, etc.
    */
  private long resultSetColToLong(ResultSet rs, int colNum, int sqlDataType) throws SQLException {
    try {
      switch (sqlDataType) {
      case Types.DATE:
        return rs.getDate(colNum).getTime();
      case Types.TIME:
        return rs.getTime(colNum).getTime();
      case Types.TIMESTAMP:
        return rs.getTimestamp(colNum).getTime();
      default:
        throw new SQLException("Not a date-type field");
      }
    } catch (NullPointerException npe) {
      // null column. return minimum long value.
      LOG.warn("Encountered a NULL date in the split column. Splits may be poorly balanced.");
      return Long.MIN_VALUE;
    }
  }

  /**  Parse the long-valued timestamp into the appropriate SQL date type. */
  private Date longToDate(long val, int sqlDataType) {
    switch (sqlDataType) {
    case Types.DATE:
      return new java.sql.Date(val);
    case Types.TIME:
      return new java.sql.Time(val);
    case Types.TIMESTAMP:
      return new java.sql.Timestamp(val);
    default: // Shouldn't ever hit this case.
      return null;
    }
  }

  /**
   * Given a Date 'd', format it as a string for use in a SQL date
   * comparison operation.
   * @param d the date to format.
   * @return the string representing this date in SQL with any appropriate
   * quotation characters, etc.
   */
  protected String dateToString(Date d) {
    return "'" + d.toString() + "'";
  }
}
