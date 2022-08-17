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

package org.apache.spark.sql.util;

import org.apache.spark.sql.types.Int128;

public class Int128Holder {
  private long high;
  private long low;

  public Int128Holder() {
  }

  public long high() {
    return high;
  }

  public void high(long high) {
    this.high = high;
  }

  public long low() {
    return low;
  }

  public void low(long low) {
    this.low = low;
  }

  public void set(long high, long low) {
    this.high = high;
    this.low = low;
  }

  public void set(Int128 value) {
    this.high = value.high();
    this.low = value.low();
  }

  public Int128 get() {
    return new Int128().set(high, low);
  }
}
