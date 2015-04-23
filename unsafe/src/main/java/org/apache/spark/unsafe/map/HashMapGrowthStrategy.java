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

package org.apache.spark.unsafe.map;

/**
 * Interface that defines how we can grow the size of a hash map when it is over a threshold.
 */
public interface HashMapGrowthStrategy {

  int nextCapacity(int currentCapacity);

  /**
   * Double the size of the hash map every time.
   */
  HashMapGrowthStrategy DOUBLING = new Doubling();

  class Doubling implements HashMapGrowthStrategy {
    @Override
    public int nextCapacity(int currentCapacity) {
      return currentCapacity * 2;
    }
  }

}
