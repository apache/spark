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

package org.apache.spark.sql.catalyst.data;

/**
 * Common setter methods for internal container classes, like {@link ArrayData} and
 * {@link InternalRow}.
 */
public interface SpecializedSetters {
  /**
   * Sets whether the value at the given ordinal is NULL.
   * @param ordinal
   */
  void setNullAt(int ordinal);

  /**
   * Updates the value at the given ordinal.
   * <p>
   * Note that after updating, the given value will be kept in this row, and the caller side should
   * guarantee that this value won't be changed afterwards.
   *
   * @param ordinal index in the row to set
   * @param value value to set at index i
   */
  void update(int ordinal, Object value);

  /**
   * Updates the boolean value at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index in the row to set
   * @param value boolean value to set
   */
  default void setBoolean(int ordinal, boolean value) {
    update(ordinal, value);
  }

  /**
   * Updates the byte value at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index in the row to set
   * @param value byte value to set
   */
  default void setByte(int ordinal, byte value) {
    update(ordinal, value);
  }

  /**
   * Updates the short value at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index in the row to set
   * @param value short value to set
   */
  default void setShort(int ordinal, short value) {
    update(ordinal, value);
  }

  /**
   * Updates the int value at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index in the row to set
   * @param value int value to set
   */
  default void setInt(int ordinal, int value) {
    update(ordinal, value);
  }

  /**
   * Updates the long value at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index in the row to set
   * @param value long value to set
   */
  default void setLong(int ordinal, long value) {
    update(ordinal, value);
  }

  /**
   * Updates the float value at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index in the row to set
   * @param value float value to set
   */
  default void setFloat(int ordinal, float value) {
    update(ordinal, value);
  }

  /**
   * Updates the double value at the given ordinal, avoiding boxing if possible.
   *
   * @param ordinal index in the row to set
   * @param value double value to set
   */
  default void setDouble(int ordinal, double value) {
    update(ordinal, value);
  }
}
