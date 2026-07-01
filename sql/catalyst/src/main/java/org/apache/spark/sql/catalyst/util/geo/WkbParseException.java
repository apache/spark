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
public class WkbParseException extends RuntimeException {
  private final String parseError;
  private final long position;
  private final byte[] wkb;

  WkbParseException(String parseError, long position, byte[] wkb) {
    super();
    this.parseError = parseError;
    this.position = position;
    this.wkb = wkb;
  }

  public String getParseError() {
    return parseError;
  }

  public long getPosition() {
    return position;
  }

  public byte[] getWkb() {
    return wkb;
  }
}
