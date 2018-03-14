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
package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Base class for writing Unsafe* structures.
 */
public abstract class UnsafeWriter {
  public abstract void setNull1Bytes(int ordinal);
  public abstract void setNull2Bytes(int ordinal);
  public abstract void setNull4Bytes(int ordinal);
  public abstract void setNull8Bytes(int ordinal);
  public abstract void write(int ordinal, boolean value);
  public abstract void write(int ordinal, byte value);
  public abstract void write(int ordinal, short value);
  public abstract void write(int ordinal, int value);
  public abstract void write(int ordinal, long value);
  public abstract void write(int ordinal, float value);
  public abstract void write(int ordinal, double value);
  public abstract void write(int ordinal, Decimal input, int precision, int scale);
  public abstract void write(int ordinal, UTF8String input);
  public abstract void write(int ordinal, byte[] input);
  public abstract void write(int ordinal, CalendarInterval input);
  public abstract void setOffsetAndSize(int ordinal, int currentCursor, int size);
}
