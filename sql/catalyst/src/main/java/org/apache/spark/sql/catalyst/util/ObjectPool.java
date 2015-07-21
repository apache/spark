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
 * A object pool stores a collection of objects in array, then they can be referenced by the
 * pool plus an index.
 */
public class ObjectPool {

  /**
   * An array to hold objects, which will grow as needed.
   */
  private Object[] objects;

  /**
   * How many objects in the pool.
   */
  private int numObj;

  public ObjectPool(int capacity) {
    objects = new Object[capacity];
    numObj = 0;
  }

  /**
   * Returns how many objects in the pool.
   */
  public int size() {
    return numObj;
  }

  /**
   * Returns the object at position `idx` in the array.
   */
  public Object get(int idx) {
    assert (idx < numObj);
    return objects[idx];
  }

  /**
   * Puts an object `obj` at the end of array, returns the index of it.
   * <p/>
   * The array will grow as needed.
   */
  public int put(Object obj) {
    if (numObj >= objects.length) {
      Object[] tmp = new Object[objects.length * 2];
      System.arraycopy(objects, 0, tmp, 0, objects.length);
      objects = tmp;
    }
    objects[numObj++] = obj;
    return numObj - 1;
  }

  /**
   * Replaces the object at `idx` with new one `obj`.
   */
  public void replace(int idx, Object obj) {
    assert (idx < numObj);
    objects[idx] = obj;
  }
}
