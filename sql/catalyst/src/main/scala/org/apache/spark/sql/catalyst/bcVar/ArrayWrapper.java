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

package org.apache.spark.sql.catalyst.bcVar;

public class ArrayWrapper {
  private final Object base;
  private final int relativeIndex;
  private final boolean is1Dim;

  public ArrayWrapper(Object base, int relativeIndex, boolean is1Dim) {
    this.base = base;
    this.relativeIndex = relativeIndex;
    this.is1Dim = is1Dim;
  }

  public Object get(int pos) {
    if (this.is1Dim) {
      return ((Object[]) this.base)[pos];
    } else {
      return ((Object[][])this.base)[pos][this.relativeIndex];
    }
  }

  public int getLength() {
    if (this.is1Dim) {
      return ((Object[]) this.base).length;
    } else {
      return ((Object[][])this.base).length;
    }
  }

  public boolean isOneDimensional() {
    return this.is1Dim;
  }

  public Object[] getBaseAs1DArray() {
    return (Object[]) this.base;
  }

  public Object[][] getBaseAs2DArray() {
    if (this.is1Dim) {
      throw new IllegalStateException("cannot cast a 1D array to 2D");
    } else {
      return (Object[][]) this.base;
    }
  }
}
