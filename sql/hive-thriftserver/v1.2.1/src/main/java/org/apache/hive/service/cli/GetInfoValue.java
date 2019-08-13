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

package org.apache.hive.service.cli;

import org.apache.hive.service.cli.thrift.TGetInfoValue;

/**
 * GetInfoValue.
 *
 */
public class GetInfoValue {
  private String stringValue = null;
  private short shortValue;
  private int intValue;
  private long longValue;

  public GetInfoValue(String stringValue) {
    this.stringValue = stringValue;
  }

  public GetInfoValue(short shortValue) {
    this.shortValue = shortValue;
  }

  public GetInfoValue(int intValue) {
    this.intValue = intValue;
  }

  public GetInfoValue(long longValue) {
    this.longValue = longValue;
  }

  public GetInfoValue(TGetInfoValue tGetInfoValue) {
    switch (tGetInfoValue.getSetField()) {
    case STRING_VALUE:
      stringValue = tGetInfoValue.getStringValue();
      break;
    default:
      throw new IllegalArgumentException("Unreconigzed TGetInfoValue");
    }
  }

  public TGetInfoValue toTGetInfoValue() {
    TGetInfoValue tInfoValue = new TGetInfoValue();
    if (stringValue != null) {
      tInfoValue.setStringValue(stringValue);
    }
    return tInfoValue;
  }

  public String getStringValue() {
    return stringValue;
  }

  public short getShortValue() {
    return shortValue;
  }

  public int getIntValue() {
    return intValue;
  }

  public long getLongValue() {
    return longValue;
  }
}
