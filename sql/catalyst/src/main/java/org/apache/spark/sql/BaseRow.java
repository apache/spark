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

package org.apache.spark.sql;

import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;

public abstract class BaseRow extends InternalRow {

  @Override
  final public int length() {
    return size();
  }

  @Override
  public StructType schema() { throw new UnsupportedOperationException(); }

  @Override
  final public Object apply(int i) {
    return get(i);
  }

  @Override
  public int getInt(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean(int i) {
    throw new UnsupportedOperationException();
  }

  /**
   * A generic version of Row.equals(Row), which is used for tests.
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof InternalRow) {
      InternalRow row = (InternalRow) other;
      int n = size();
      if (n != row.size()) {
        return false;
      }
      for (int i = 0; i < n; i ++) {
        if (isNullAt(i) != row.isNullAt(i) || (!isNullAt(i) && !get(i).equals(row.get(i)))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public InternalRow copy() {
    final int n = size();
    Object[] arr = new Object[n];
    for (int i = 0; i < n; i++) {
      arr[i] = get(i);
    }
    return new GenericRow(arr);
  }

  public Seq<Object> toSeq() {
    final int n = size();
    final ArraySeq<Object> values = new ArraySeq<Object>(n);
    for (int i = 0; i < n; i++) {
      values.update(i, get(i));
    }
    return values;
  }
}
