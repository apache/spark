/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.operation;

public class MetadataOperationUtils {

  /**
   * Convert wildchars and escape sequence from JDBC format to datanucleous/regex
   */
  public static String legacyConvertIdentifierPattern(
      final String pattern, boolean datanucleusFormat) {
    if (pattern == null) {
      return legacyConvertPattern("%", true);
    } else {
      return legacyConvertPattern(pattern, datanucleusFormat);
    }
  }

  /**
   * Convert wildchars and escape sequence of schema pattern from JDBC format to datanucleous/regex
   * The schema pattern treats empty string also as wildchar
   */
  public static String legacyConvertSchemaPattern(final String pattern) {
    if ((pattern == null) || pattern.isEmpty()) {
      return legacyConvertPattern("%", true);
    } else {
      return legacyConvertPattern(pattern, true);
    }
  }

  /**
   * Convert a pattern containing JDBC catalog search wildcards into
   * Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   *                these characters escaped using "\".
   * @return replace %/_ with regex search characters, also handle escaped characters.
   *
   * The datanucleus module expects the wildchar as '*'. The columns search on the
   * other hand is done locally inside the hive code and that requires the regex wildchar
   * format '.*'  This is driven by the datanucleusFormat flag.
   */
  public static String legacyConvertPattern(final String pattern, boolean datanucleusFormat) {
    String wStr;
    if (datanucleusFormat) {
      wStr = "*";
    } else {
      wStr = ".*";
    }
    return pattern
        .replaceAll("([^\\\\])%", "$1" + wStr).replaceAll("\\\\%", "%").replaceAll("^%", wStr)
        .replaceAll("([^\\\\])_", "$1.").replaceAll("\\\\_", "_").replaceAll("^_", ".");
  }

  public static String convertIdentifierPattern(
      final String pattern, boolean datanucleusFormat) {
    if (pattern == null) {
      return convertPattern("%", true);
    } else {
      return convertPattern(pattern, datanucleusFormat);
    }
  }

  public static String convertSchemaPattern(final String pattern, boolean datanucleusFormat) {
    if ((pattern == null) || pattern.isEmpty()) {
      String all = datanucleusFormat? "*" : ".*";
      return convertPattern(all, datanucleusFormat);
    } else {
      return convertPattern(pattern, datanucleusFormat);
    }
  }

  public static String convertPattern(final String pattern, boolean datanucleusFormat) {
    if (datanucleusFormat) {
      return pattern
          .replaceAll("([^\\\\])\\*", "$1%")
          .replaceAll("\\\\\\*", "*")
          .replaceAll("^\\*", "%")
          .replaceAll("([^\\\\])\\.", "$1_")
          .replaceAll("\\\\\\.", ".")
          .replaceAll("^\\.", "_");
    } else {
      return pattern
          .replaceAll("([^\\\\])\\.\\*", "$1%")
          .replaceAll("\\\\\\.\\*", "*")
          .replaceAll("^\\.\\*", "%")
          .replaceAll("([^\\\\])\\.", "$1_")
          .replaceAll("\\\\\\.", ".")
          .replaceAll("^\\.", "_");
    }
  }
}
