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
package org.apache.spark.network.util;

public enum ByteUnit {
  BYTE (1, "Bytes"),
  KiB (1024, "KiB"),
  MiB (Math.pow(1024, 2), "MiB"),
  GiB (Math.pow(1024, 3), "GiB"),
  TiB (Math.pow(1024, 4), "TiB"),
  PiB (Math.pow(1024, 5), "PiB");

  private ByteUnit(double multiplier, String name) {
    this.multiplier = multiplier;
    this.name = name;
  }

  // Interpret the provided number (d) with suffix (u) as this unit type.
  // E.g. KiB.interpret(1, MiB) interprets 1MiB as its KiB representation = 1024k
  public long convertFrom(long d, ByteUnit u) {
    double converted = u.toBytes(d) / multiplier;
    if (converted > Long.MAX_VALUE)
      throw new IllegalArgumentException("Converted value (" + converted + ") " +
        "exceeds Long.MAX_VALUE for " + name + ". Try a larger suffix (e.g. MiB instead of KiB)");
    return (long) converted;  
  }
  
  // Convert the provided number (d) interpreted as this unit type to unit type (u). 
  public long convertTo(long d, ByteUnit u) {
    double converted = toBytes(d) / u.multiplier;
    if (converted > Long.MAX_VALUE)
      throw new IllegalArgumentException("Converted value (" + converted + ") " +
        "exceeds Long.MAX_VALUE for " + u.name + ". Try a larger suffix (e.g. MiB instead of KiB)");

    return (long) converted;
  }

  public double toBytes(long d) {
    if (d < 0) {
      throw new IllegalArgumentException("Negative size value. Size must be positive: " + d);
    }
    return d * multiplier; 
  }
  
  public long toKiB(long d) { return convertTo(d, KiB); }
  public long toMiB(long d) { return convertTo(d, MiB); }
  public long toGiB(long d) { return convertTo(d, GiB); }
  public long toTiB(long d) { return convertTo(d, TiB); }
  public long toPiB(long d) { return convertTo(d, PiB); }
  
  private double multiplier = 0;
  private String name = "";
}
