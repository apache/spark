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

package org.apache.spark.sql.api.java;

/**
 * The data type representing java.math.BigDecimal values.
 */
public class DecimalType extends DataType {
  private boolean hasPrecisionInfo;
  private int precision;
  private int scale;

  public DecimalType(int precision, int scale) {
    this.hasPrecisionInfo = true;
    this.precision = precision;
    this.scale = scale;
  }

  public DecimalType() {
    this.hasPrecisionInfo = false;
    this.precision = -1;
    this.scale = -1;
  }

  public boolean isUnlimited() {
    return !hasPrecisionInfo;
  }

  public boolean isFixed() {
    return hasPrecisionInfo;
  }

  /** Return the precision, or -1 if no precision is set */
  public int getPrecision() {
    return precision;
  }

  /** Return the scale, or -1 if no precision is set */
  public int getScale() {
    return scale;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DecimalType that = (DecimalType) o;

    if (hasPrecisionInfo != that.hasPrecisionInfo) return false;
    if (precision != that.precision) return false;
    if (scale != that.scale) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (hasPrecisionInfo ? 1 : 0);
    result = 31 * result + precision;
    result = 31 * result + scale;
    return result;
  }
}
