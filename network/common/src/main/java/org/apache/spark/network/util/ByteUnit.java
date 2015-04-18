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

/**
 * Code based on https://github.com/fabian-barney/Utils
 * Copyright 2011 Fabian Barney
 *
 * @author Fabian Barney
 */
public enum ByteUnit {
  /**
   * <pre>
   * Byte (B)
   * 1 Byte
   */
  BYTE {
    @Override
    public double toBytes(double d) {
      return d;
    }

    @Override
    public double convert(double d, ByteUnit u) {
      return u.toBytes(d);
    }
  },

  /**
   * <pre>
   * Kilobyte (kB) = 1024 Byte
   */
  KB {
    @Override
    public double toBytes(double d) {
      return safeMulti(d, C_KB);
    }

    @Override
    public double convert(double d, ByteUnit u) {
      return u.toKB(d);
    }
  },

  /**
   * <pre>
   * Megabyte (MB) = 1024 * 1024 Byte
   */
  MB {
    @Override
    public double toBytes(double d) {
      return safeMulti(d, C_MB);
    }

    @Override
    public double convert(double d, ByteUnit u) {
      return u.toMB(d);
    }
  },

  /**
   * <pre>
   * Gigabyte (GB) = 1024 * 1024 * 1024 Byte
   */
  GB {
    @Override
    public double toBytes(double d) {
      return safeMulti(d, C_GB);
    }

    @Override
    public double convert(double d, ByteUnit u) { return u.toGB(d); }
  },

  /**
   * <pre>
   * Terabyte (TB) = 1024 * 1024 * 1024 * 1024 Byte
   */
  TB {
    @Override
    public double toBytes(double d) {
      return safeMulti(d, C_TB);
    }

    @Override
    public double convert(double d, ByteUnit u) {
      return u.toTB(d);
    }
  },

  /**
   * <pre>
   * Petabyte (PB) = 1024 * 1024 * 1024 * 1024 * 1024 Byte
   */
  PB {
    @Override
    public double toBytes(double d) {
      return safeMulti(d, C_PB);
    }

    @Override
    public double convert(double d, ByteUnit u) {
      return u.toPB(d);
    }
  };

  static final double C_KB = 1024d;
  static final double C_MB = Math.pow(1024d, 2d);
  static final double C_GB = Math.pow(1024d, 3d);
  static final double C_TB = Math.pow(1024d, 4d);
  static final double C_PB = Math.pow(1024d, 5d);

  private static final double MAX = Double.MAX_VALUE;

  static double safeMulti(double d, double multi) {
    double limit = MAX / multi;

    if (d > limit) {
      return Double.MAX_VALUE;
    }
    if (d < -limit) {
      return Double.MIN_VALUE;
    }

    return d * multi;
  }

  public abstract double toBytes(double d);

  public final double toKB(double d) {
    return toBytes(d) / C_KB;
  }

  public final double toMB(double d) {
    return toBytes(d) / C_MB;
  }

  public final double toGB(double d) {
    return toBytes(d) / C_GB;
  }

  public final double toTB(double d) {
    return toBytes(d) / C_TB;
  }

  public final double toPB(double d) {
    return toBytes(d) / C_PB;
  }

  public abstract double convert(double d, ByteUnit u);
}
