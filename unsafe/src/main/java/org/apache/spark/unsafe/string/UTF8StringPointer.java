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

package org.apache.spark.unsafe.string;

import javax.annotation.Nullable;

/**
 * A pointer to UTF8String data.
 */
public class UTF8StringPointer {

  @Nullable
  protected Object obj;
  protected long offset;
  protected int lengthInBytes;

  public UTF8StringPointer() { }

  public void set(Object obj, long offset, int lengthInBytes) {
    this.obj = obj;
    this.offset = offset;
    this.lengthInBytes = lengthInBytes;
  }

  public int getLengthInCodePoints() {
    return UTF8StringMethods.getLengthInCodePoints(obj, offset, lengthInBytes);
  }

  public int getLengthInBytes() { return lengthInBytes; }

  public Object getBaseObject() { return obj; }

  public long getBaseOffset() { return offset; }

  public String toJavaString() {
    return UTF8StringMethods.toJavaString(obj, offset, lengthInBytes);
  }

  @Override public String toString() { return toJavaString(); }
}
