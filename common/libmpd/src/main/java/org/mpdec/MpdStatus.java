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
package org.mpdec;

/** Sticky IEEE / GDA status flags. */
public final class MpdStatus {
  public static final int CONVERSION_SYNTAX = 1;
  public static final int DIVISION_BY_ZERO = 1 << 1;
  public static final int DIVISION_IMPOSSIBLE = 1 << 2;
  public static final int DIVISION_UNDEFINED = 1 << 3;
  public static final int INVALID_OPERATION = 1 << 4;
  public static final int INVALID_CONTEXT = 1 << 5;
  public static final int OVERFLOW = 1 << 6;
  public static final int UNDERFLOW = 1 << 7;
  public static final int INEXACT = 1 << 8;
  public static final int ROUNDED = 1 << 9;
  public static final int SUBNORMAL = 1 << 10;
  public static final int CLAMPED = 1 << 11;

  private int flags;

  public void clear() {
    flags = 0;
  }

  public int get() {
    return flags;
  }

  public void set(int bits) {
    flags |= bits;
  }

  public boolean test(int bits) {
    return (flags & bits) != 0;
  }
}
