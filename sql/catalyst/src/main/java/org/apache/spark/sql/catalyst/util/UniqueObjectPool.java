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

import java.util.HashMap;

/**
 * An unique object pool stores a collection of unique objects in it.
 */
public class UniqueObjectPool extends ObjectPool {

  /**
   * A hash map from objects to their indexes in the array.
   */
  private HashMap<Object, Integer> objIndex;

  public UniqueObjectPool(int capacity) {
    super(capacity);
    objIndex = new HashMap<Object, Integer>();
  }

  /**
   * Put an object `obj` into the pool. If there is an existing object equals to `obj`, it will
   * return the index of the existing one.
   */
  @Override
  public int put(Object obj) {
    if (objIndex.containsKey(obj)) {
      return objIndex.get(obj);
    } else {
      int idx = super.put(obj);
      objIndex.put(obj, idx);
      return idx;
    }
  }

  /**
   * The objects can not be replaced.
   */
  @Override
  public void replace(int idx, Object obj) {
    throw new UnsupportedOperationException();
  }
}
