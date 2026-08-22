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

package org.apache.hive.service.cli;

import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * CLIServiceUtils.
 *
 */
public class CLIServiceUtils {


  private static final char SEARCH_STRING_ESCAPE = '\\';
  public static final StringLayout verboseLayout = PatternLayout.newBuilder().withPattern(
    "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n%ex").build();
  public static final StringLayout nonVerboseLayout = PatternLayout.newBuilder().withPattern(
    "%-5p : %m%n%ex").build();

  /**
   * Convert a SQL search pattern into an equivalent Java Regex.
   *
   * Per JDBC spec, only '%' (match any substring) and '_' (match any single character) are
   * wildcard characters. All other characters — including regex metacharacters like '*', '.',
   * '(', '[', '+', '?', '^', '$', '{', '|' — are treated as literals.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   * these characters escaped using {@code getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, escape all other regex-special characters,
   * and handle escaped characters.
   */
  public static String patternToRegex(String pattern) {
    if (pattern == null) {
      return ".*";
    } else {
      StringBuilder result = new StringBuilder(pattern.length());

      boolean escaped = false;
      for (int i = 0, len = pattern.length(); i < len; i++) {
        char c = pattern.charAt(i);
        if (escaped) {
          if (c != SEARCH_STRING_ESCAPE) {
            escaped = false;
          }
          appendLiteralChar(result, Character.toLowerCase(c));
        } else {
          if (c == SEARCH_STRING_ESCAPE) {
            escaped = true;
            continue;
          } else if (c == '%') {
            result.append(".*");
          } else if (c == '_') {
            result.append('.');
          } else {
            appendLiteralChar(result, Character.toLowerCase(c));
          }
        }
      }
      return result.toString();
    }
  }

  // Regex metacharacters that must be escaped with a backslash to be treated as literals.
  private static final String REGEX_METACHARACTERS = "\\.[]{}()*+?^$|";

  /**
   * Append a character to the result, escaping it if it is a regex metacharacter.
   */
  private static void appendLiteralChar(StringBuilder sb, char c) {
    if (REGEX_METACHARACTERS.indexOf(c) >= 0) {
      sb.append('\\');
    }
    sb.append(c);
  }

}
