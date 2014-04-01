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

import java.util.Properties;
import java.util.regex.Pattern;

class ParsedLine {
  Properties content;
  LogRecordType type;

  static final Pattern keyValPair = Pattern
      .compile(" *([a-zA-Z0-9_]+)=\"((?:[^\"\\\\]|\\\\[ .\"\\\\])*)\"");

  @SuppressWarnings("unused") 
  ParsedLine(String fullLine, int version) {
    super();

    content = new Properties();

    int firstSpace = fullLine.indexOf(" ");

    if (firstSpace < 0) {
      firstSpace = fullLine.length();
    }

    if (firstSpace == 0) {
      return; // This is a junk line of some sort
    }

    type = LogRecordType.intern(fullLine.substring(0, firstSpace));

    String propValPairs = fullLine.substring(firstSpace + 1);

    while (propValPairs.length() > 0 && propValPairs.charAt(0) == ' ') {
      propValPairs = propValPairs.substring(1);
    }

    int cursor = 0;

    while (cursor < propValPairs.length()) {
      int equals = propValPairs.indexOf('=', cursor);

      if (equals < 0) {
        // maybe we do some error processing
        return;
      }

      int nextCursor;

      int endValue;

      if (propValPairs.charAt(equals + 1) == '\"') {
        int closeQuote = propValPairs.indexOf('\"', equals + 2);

        nextCursor = closeQuote + 1;

        endValue = closeQuote;

        if (closeQuote < 0) {
          endValue = propValPairs.length();

          nextCursor = endValue;
        }
      } else {
        int closeSpace = propValPairs.indexOf(' ', equals + 1);

        if (closeSpace < 0) {
          closeSpace = propValPairs.length();
        }

        endValue = closeSpace;

        nextCursor = endValue;
      }

      content.setProperty(propValPairs.substring(cursor, equals), propValPairs
          .substring(equals + 2, endValue));

      cursor = nextCursor;

      while (cursor < propValPairs.length()
          && propValPairs.charAt(cursor) == ' ') {
        ++cursor;
      }
    }
  }

  protected LogRecordType getType() {
    return type;
  }

  protected String get(String key) {
    return content.getProperty(key);
  }

  protected long getLong(String key) {
    String val = get(key);

    return Long.parseLong(val);
  }
}
