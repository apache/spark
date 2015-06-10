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

import org.apache.spark.sql.catalyst.expressions.MutableRow;

public abstract class BaseMutableRow extends BaseRow implements MutableRow {

  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(int ordinal, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(int ordinal, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDouble(int ordinal, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setShort(int ordinal, short value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setByte(int ordinal, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFloat(int ordinal, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setString(int ordinal, String value) {
    throw new UnsupportedOperationException();
  }
}
