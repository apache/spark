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
  BYTE (1),
  KiB (1024l),
  MiB ((long) Math.pow(1024l, 2l)),
  GiB ((long) Math.pow(1024l, 3l)),
  TiB ((long) Math.pow(1024l, 4l)),
  PiB ((long) Math.pow(1024l, 5l));

  private ByteUnit(long multiplier) {
    this.multiplier = multiplier;
  }

  // Interpret the provided number (d) with suffix (u) as this unit type.
  // E.g. KiB.interpret(1, MiB) interprets 1MiB as its KiB representation = 1024k
  public long interpret(long d, ByteUnit u) {
    return u.toBytes(d) / multiplier;  
  }
  
  // Convert the provided number (d) interpreted as this unit type to unit type (u). 
  public long convert(long d, ByteUnit u) {
    return toBytes(d) / u.multiplier;
  }

  public long toBytes(long d) { return x(d, multiplier); }
  public long toKiB(long d) { return convert(d, KiB); }
  public long toMiB(long d) { return convert(d, MiB); }
  public long toGiB(long d) { return convert(d, GiB); }
  public long toTiB(long d) { return convert(d, TiB); }
  public long toPiB(long d) { return convert(d, PiB); }
  
  long multiplier = 0;
  static final long MAX = Long.MAX_VALUE;

  /**
   * Scale d by m, checking for overflow.
   * This has a short name to make above code more readable.
   */
  static long x(long d, long m) {
    if (d == 0) { return 0; }
    long over = MAX / d;
    if (d >  over) return Long.MAX_VALUE;
    if (d < -over) return Long.MIN_VALUE;
    return d * m;
  }
}
