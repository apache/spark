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
  /** Byte (B) */
  BYTE {
    public long toBytes(long d) { return d; }
    
    public long convert(long d, ByteUnit u) { return u.toBytes(d); }
  },

  /** Kibibyte (KiB) = 1024 Byte */
  KiB {
    public long toBytes(long d) { return x(d, C_KiB); }
    
    public long convert(long d, ByteUnit u) { return u.toKiB(d); }
  },

  /** Mebibyte (MiB) = (1024^2) Byte */
  MiB {
    public long toBytes(long d) { return x(d, C_MiB); }
    
    public long convert(long d, ByteUnit u) { return u.toMiB(d); }
  },

  /** Gibibyte (GiB) = (1024^3) Byte */
  GiB {
    public long toBytes(long d) { return x(d, C_GiB);
    }

    public long convert(long d, ByteUnit u) { return u.toGiB(d); }
  },

  /** Tebibyte (TiB) = (1024^4) Byte */
  TiB {
    public long toBytes(long d) { return x(d, C_TiB); }
    
    public long convert(long d, ByteUnit u) { return u.toTiB(d); }
  },

  /** Pebibyte (PB) = (1024^5) Byte */
  PiB {
    public long toBytes(long d) { return x(d, C_PiB); }
    
    public long convert(long d, ByteUnit u) { return u.toPiB(d); }
  };

  static final long C_KiB = 1024l;
  static final long C_MiB = (long) Math.pow(1024l, 2l);
  static final long C_GiB = (long) Math.pow(1024l, 3l);
  static final long C_TiB = (long) Math.pow(1024l, 4l);
  static final long C_PiB = (long) Math.pow(1024l, 5l);

  static final long MAX = Long.MAX_VALUE;

  /**
   * Scale d by m, checking for overflow.
   * This has a short name to make above code more readable.
   */
  static long x(long d, long m) {
    long over = MAX / d;
    if (d >  over) return Long.MAX_VALUE;
    if (d < -over) return Long.MIN_VALUE;
    return d * m;
  }

  public long convert(long d, ByteUnit u) { throw new AbstractMethodError(); }
  
  public long toBytes(long d) { throw new AbstractMethodError(); }
  
  public long toKiB(long d) { return toBytes(d) / C_KiB; }

  public long toMiB(long d) { return toBytes(d) / C_MiB; }

  public long toGiB(long d) { return toBytes(d) / C_GiB; }

  public long toTiB(long d) { return toBytes(d) / C_TiB; }

  public long toPiB(long d) { return toBytes(d) / C_PiB; }
}
