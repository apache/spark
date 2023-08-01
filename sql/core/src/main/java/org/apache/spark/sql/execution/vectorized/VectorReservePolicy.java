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

package org.apache.spark.sql.execution.vectorized;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.unsafe.array.ByteArrayMethods;

public abstract class VectorReservePolicy {

  protected int defaultCapacity;

  /**
   * Upper limit for the maximum capacity for this column.
   */
  @VisibleForTesting
  protected int MAX_CAPACITY = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

  /**
   * True if this column has default values. Return the default values instead of NULL when the
   * corresponding columns are not present in storage. We can not reset the data of column vectors
   * that has default values.
   */
  protected boolean hasDefaultValue = false;

  abstract int nextCapacity(int requiredCapacity);

  abstract boolean shouldCleanData();
}

class DefaultVectorReservePolicy extends VectorReservePolicy {

  private int hugeThreshold;
  private double hugeReserveRatio;
  private int currentCapacity;

  DefaultVectorReservePolicy(int defaultCapacity) {
    this.defaultCapacity = defaultCapacity;
    this.currentCapacity = defaultCapacity;
    SQLConf conf = SQLConf.get();
    hugeThreshold = conf.vectorizedHugeVectorThreshold();
    hugeReserveRatio = conf.vectorizedHugeVectorReserveRatio();
  }

  @Override
  public int nextCapacity(int requiredCapacity) {
    if (hugeThreshold < 0 || requiredCapacity < hugeThreshold) {
      currentCapacity = (int) Math.min(MAX_CAPACITY, requiredCapacity * 2L);
    } else {
      currentCapacity = (int) Math.min(MAX_CAPACITY,
          (requiredCapacity - hugeThreshold) * hugeReserveRatio + hugeThreshold * 2L);
    }
    return currentCapacity;
  }

  @Override
  public boolean shouldCleanData() {
    return hugeThreshold > 0 && !hasDefaultValue && currentCapacity > hugeThreshold;
  }
}
