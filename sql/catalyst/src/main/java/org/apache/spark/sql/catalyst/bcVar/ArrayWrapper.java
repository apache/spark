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

public interface ArrayWrapper {
  Object get(int pos);

  int getLength();

  boolean isOneDimensional();

  Object[] getBaseAs1DArray();

  Object[][] getBaseAs2DArray();

  static ArrayWrapper wrapArray(Object source, boolean is1Dimensional, int relativeIndex) {
    return is1Dimensional ? new OneDimensionArrayWrapper((Object[]) source, relativeIndex) :
      new TwoDimensionArrayWrapper((Object[][]) source, relativeIndex);
  }
}

class OneDimensionArrayWrapper implements ArrayWrapper {
  private final Object[] base;
  private final int relativeIndex;

  OneDimensionArrayWrapper(Object[] base, int relativeIndex) {
    this.base = base;
    this.relativeIndex = relativeIndex;
  }

  @Override
  public Object get(int pos) {
    return this.base[pos];
  }

  @Override
  public int getLength() {
    return this.base.length;
  }

  @Override
  public boolean isOneDimensional() {
    return true;
  }

  @Override
  public Object[] getBaseAs1DArray() {
    return this.base;
  }

  @Override
  public Object[][] getBaseAs2DArray() {
    throw new IllegalStateException("cannot cast a 1D array to 2D");
  }
}

class TwoDimensionArrayWrapper implements ArrayWrapper {
  private final Object[][] base;
  private final int relativeIndex;

  TwoDimensionArrayWrapper(Object[][] base, int relativeIndex) {
    this.base = base;
    this.relativeIndex = relativeIndex;
  }

  @Override
  public Object get(int pos) {
    return this.base[pos][this.relativeIndex];
  }

  @Override
  public int getLength() {
    return this.base.length;
  }

  @Override
  public boolean isOneDimensional() {
    return false;
  }

  @Override
  public Object[] getBaseAs1DArray() {
    return this.base;
  }

  @Override
  public Object[][] getBaseAs2DArray() {
    return this.base;
  }
}
