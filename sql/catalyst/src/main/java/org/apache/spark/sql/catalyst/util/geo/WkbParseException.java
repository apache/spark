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
package org.apache.spark.sql.catalyst.util.geo;

/**
 * Exception thrown when parsing WKB data fails.
 */
class WkbParseException extends RuntimeException {
  private final long position;
  private final String wkbString;

  WkbParseException(String message, long position, byte[] wkb) {
    super(formatMessage(message, position, wkb));
    this.position = position;
    this.wkbString = wkb != null ? bytesToHex(wkb) : "";
  }

  private static String formatMessage(String message, long position, byte[] wkb) {
    String baseMessage = message + " at position " + position;
    if (wkb != null && wkb.length > 0) {
      baseMessage += " in WKB: " + bytesToHex(wkb);
    }
    return baseMessage;
  }

  private static String bytesToHex(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }

  long getPosition() {
    return position;
  }

  String getWkbString() {
    return wkbString;
  }
}
