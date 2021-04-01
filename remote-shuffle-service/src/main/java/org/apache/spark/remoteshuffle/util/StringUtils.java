/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.util;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;

public class StringUtils {
  private static final int KB_SIZE = 1024;
  private static final int MB_SIZE = 1024 * 1024;
  private static final int GB_SIZE = 1024 * 1024 * 1024;

  public static Long getBytesValue(String str) {
    if (str == null || str.isEmpty()) {
      return null;
    }

    String strLower = str.toLowerCase();
    int scale = 1;

    {
      if (strLower.endsWith("kb")) {
        strLower = strLower.substring(0, strLower.length() - 2).trim();
        scale = KB_SIZE;
      }
      if (strLower.endsWith("k")) {
        strLower = strLower.substring(0, strLower.length() - 1).trim();
        scale = KB_SIZE;
      } else if (strLower.endsWith("mb")) {
        strLower = strLower.substring(0, strLower.length() - 2).trim();
        scale = MB_SIZE;
      } else if (strLower.endsWith("m")) {
        strLower = strLower.substring(0, strLower.length() - 1).trim();
        scale = MB_SIZE;
      } else if (strLower.endsWith("gb")) {
        strLower = strLower.substring(0, strLower.length() - 2).trim();
        scale = GB_SIZE;
      } else if (strLower.endsWith("g")) {
        strLower = strLower.substring(0, strLower.length() - 1).trim();
        scale = GB_SIZE;
      } else if (strLower.endsWith("bytes")) {
        strLower = strLower.substring(0, strLower.length() - "bytes".length()).trim();
        scale = 1;
      }

      strLower = strLower.replace(",", "");

      if (!NumberUtils.isDigits(strLower)) {
        throw new RuntimeException("Invalid string for bytes: " + strLower);
      }

      double doubleValue = Double.parseDouble(strLower);
      return (long) (doubleValue * scale);
    }
  }

  /**
   * Make a string for a sorted integer list. e.g. for 1,2,3,5,6,7, this method will
   * return string like "1-3,5-7".
   *
   * @param list
   * @return
   */
  public static <T extends Number> String toString4SortedNumberList(List<T> list) {
    if (list == null || list.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    T rangeStart = null;
    T rangeEnd = null;
    for (T v : list) {
      if (rangeStart == null) {
        rangeStart = v;
      } else if (rangeEnd == null) {
        if (v.longValue() == rangeStart.longValue() + 1) {
          rangeEnd = v;
        } else {
          if (!first) {
            sb.append(",");
          }
          sb.append(rangeStart);
          first = false;
          rangeStart = v;
        }
      } else if (rangeEnd.longValue() == v.longValue() - 1) {
        rangeEnd = v;
      } else {
        // got one range, put input StringBuilder
        if (!first) {
          sb.append(",");
        }
        sb.append(String.format("%s-%s", rangeStart, rangeEnd));
        first = false;
        // start new range
        rangeStart = v;
        rangeEnd = null;
      }
    }

    if (rangeStart == null) {
      return sb.toString();
    } else if (rangeEnd == null) {
      if (!first) {
        sb.append(",");
      }
      sb.append(rangeStart);
      return sb.toString();
    } else {
      if (!first) {
        sb.append(",");
      }
      sb.append(String.format("%s-%s", rangeStart, rangeEnd));
    }

    return sb.toString();
  }
}
