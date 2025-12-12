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
package org.apache.spark.sql.catalyst.util;

/**
 * Enum representing the dimension of a geometry.
 */
public enum Dimension {
  UNDEFINED(0),
  DIM_2D(2),
  DIM_3DZ(3),
  DIM_3DM(3),
  DIM_4D(4);

  private final int count;

  Dimension(int count) {
    this.count = count;
  }

  public int getCount() {
    return count;
  }

  public static Dimension fromCount(int count) {
    for (Dimension dim : values()) {
      if (dim.count == count) {
        return dim;
      }
    }
    return UNDEFINED;
  }
}

