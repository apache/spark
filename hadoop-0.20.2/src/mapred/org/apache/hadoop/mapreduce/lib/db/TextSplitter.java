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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Implement DBSplitter over text strings.
 */
public class TextSplitter extends BigDecimalSplitter {

  private static final Log LOG = LogFactory.getLog(TextSplitter.class);

  /**
   * This method needs to determine the splits between two user-provided strings.
   * In the case where the user's strings are 'A' and 'Z', this is not hard; we 
   * could create two splits from ['A', 'M') and ['M', 'Z'], 26 splits for strings
   * beginning with each letter, etc.
   *
   * If a user has provided us with the strings "Ham" and "Haze", however, we need
   * to create splits that differ in the third letter.
   *
   * The algorithm used is as follows:
   * Since there are 2**16 unicode characters, we interpret characters as digits in
   * base 65536. Given a string 's' containing characters s_0, s_1 .. s_n, we interpret
   * the string as the number: 0.s_0 s_1 s_2.. s_n in base 65536. Having mapped the
   * low and high strings into floating-point values, we then use the BigDecimalSplitter
   * to establish the even split points, then map the resulting floating point values
   * back into strings.
   */
  public List<InputSplit> split(Configuration conf, ResultSet results, String colName)
      throws SQLException {

    LOG.warn("Generating splits for a textual index column.");
    LOG.warn("If your database sorts in a case-insensitive order, "
        + "this may result in a partial import or duplicate records.");
    LOG.warn("You are strongly encouraged to choose an integral split column.");

    String minString = results.getString(1);
    String maxString = results.getString(2);

    boolean minIsNull = false;

    // If the min value is null, switch it to an empty string instead for purposes
    // of interpolation. Then add [null, null] as a special case split.
    if (null == minString) {
      minString = "";
      minIsNull = true;
    }

    if (null == maxString) {
      // If the max string is null, then the min string has to be null too.
      // Just return a special split for this case.
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
      return splits;
    }

    // Use this as a hint. May need an extra task if the size doesn't
    // divide cleanly.
    int numSplits = conf.getInt("mapred.map.tasks", 1);

    String lowClausePrefix = colName + " >= '";
    String highClausePrefix = colName + " < '";

    // If there is a common prefix between minString and maxString, establish it
    // and pull it out of minString and maxString.
    int maxPrefixLen = Math.min(minString.length(), maxString.length());
    int sharedLen;
    for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++) {
      char c1 = minString.charAt(sharedLen);
      char c2 = maxString.charAt(sharedLen);
      if (c1 != c2) {
        break;
      }
    }

    // The common prefix has length 'sharedLen'. Extract it from both.
    String commonPrefix = minString.substring(0, sharedLen);
    minString = minString.substring(sharedLen);
    maxString = maxString.substring(sharedLen);

    List<String> splitStrings = split(numSplits, minString, maxString, commonPrefix);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    // Convert the list of split point strings into an actual set of InputSplits.
    String start = splitStrings.get(0);
    for (int i = 1; i < splitStrings.size(); i++) {
      String end = splitStrings.get(i);

      if (i == splitStrings.size() - 1) {
        // This is the last one; use a closed interval.
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + start + "'", colName + " <= '" + end + "'"));
      } else {
        // Normal open-interval case.
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            lowClausePrefix + start + "'", highClausePrefix + end + "'"));
      }
    }

    if (minIsNull) {
      // Add the special null split at the end.
      splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
          colName + " IS NULL", colName + " IS NULL"));
    }

    return splits;
  }

  List<String> split(int numSplits, String minString, String maxString, String commonPrefix)
      throws SQLException {

    BigDecimal minVal = stringToBigDecimal(minString);
    BigDecimal maxVal = stringToBigDecimal(maxString);

    List<BigDecimal> splitPoints = split(new BigDecimal(numSplits), minVal, maxVal);
    List<String> splitStrings = new ArrayList<String>();

    // Convert the BigDecimal splitPoints into their string representations.
    for (BigDecimal bd : splitPoints) {
      splitStrings.add(commonPrefix + bigDecimalToString(bd));
    }

    // Make sure that our user-specified boundaries are the first and last entries
    // in the array.
    if (splitStrings.size() == 0 || !splitStrings.get(0).equals(commonPrefix + minString)) {
      splitStrings.add(0, commonPrefix + minString);
    }
    if (splitStrings.size() == 1
        || !splitStrings.get(splitStrings.size() - 1).equals(commonPrefix + maxString)) {
      splitStrings.add(commonPrefix + maxString);
    }

    return splitStrings;
  }

  private final static BigDecimal ONE_PLACE = new BigDecimal(65536);

  // Maximum number of characters to convert. This is to prevent rounding errors
  // or repeating fractions near the very bottom from getting out of control. Note
  // that this still gives us a huge number of possible splits.
  private final static int MAX_CHARS = 8;

  /**
   * Return a BigDecimal representation of string 'str' suitable for use
   * in a numerically-sorting order.
   */
  BigDecimal stringToBigDecimal(String str) {
    BigDecimal result = BigDecimal.ZERO;
    BigDecimal curPlace = ONE_PLACE; // start with 1/65536 to compute the first digit.

    int len = Math.min(str.length(), MAX_CHARS);

    for (int i = 0; i < len; i++) {
      int codePoint = str.codePointAt(i);
      result = result.add(tryDivide(new BigDecimal(codePoint), curPlace));
      // advance to the next less significant place. e.g., 1/(65536^2) for the second char.
      curPlace = curPlace.multiply(ONE_PLACE);
    }

    return result;
  }

  /**
   * Return the string encoded in a BigDecimal.
   * Repeatedly multiply the input value by 65536; the integer portion after such a multiplication
   * represents a single character in base 65536. Convert that back into a char and create a
   * string out of these until we have no data left.
   */
  String bigDecimalToString(BigDecimal bd) {
    BigDecimal cur = bd.stripTrailingZeros();
    StringBuilder sb = new StringBuilder();

    for (int numConverted = 0; numConverted < MAX_CHARS; numConverted++) {
      cur = cur.multiply(ONE_PLACE);
      int curCodePoint = cur.intValue();
      if (0 == curCodePoint) {
        break;
      }

      cur = cur.subtract(new BigDecimal(curCodePoint));
      sb.append(Character.toChars(curCodePoint));
    }

    return sb.toString();
  }
}
