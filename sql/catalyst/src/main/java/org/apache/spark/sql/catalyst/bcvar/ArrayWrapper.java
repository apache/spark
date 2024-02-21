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

package org.apache.spark.sql.catalyst.bcvar;

public interface ArrayWrapper<T> {
  T get(int pos);

  int getLength();

  boolean isOneDimensional();

  Object getBaseArray();

  static  ArrayWrapper<? extends Object> wrapArray(Object source, boolean is1Dimensional,
      int index) {
    return is1Dimensional ? new OneDimensionArrayWrapper((Object[]) source) :
      new TwoDimensionArrayWrapper((Object[][]) source, index);
  }
}

class OneDimensionArrayWrapper<T> implements ArrayWrapper<T> {
  private final Object[] base;
  OneDimensionArrayWrapper(Object[] base) {
    this.base = base;
  }

  @Override
  public T get(int pos) {
    return (T)this.base[pos];
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
  public Object[] getBaseArray() {
    return this.base;
  }
}

class TwoDimensionArrayWrapper<T> implements ArrayWrapper<T> {
  private final Object[][] base;
  private final int index;

  TwoDimensionArrayWrapper(Object[][] base, int index) {
    this.base = base;
    this.index = index;
  }

  @Override
  public T get(int pos) {
    return (T)this.base[pos][this.index];
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
  public Object[][] getBaseArray() {
    return this.base;
  }
}
