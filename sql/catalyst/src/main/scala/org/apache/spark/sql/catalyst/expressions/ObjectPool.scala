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

package org.apache.spark.sql.catalyst.expressions

/**
 * A object pool stores a collection of objects in array, then they can be referenced by the
 * pool plus an index.
 *
 * @param capacity the initial capacity
 */
class ObjectPool(var capacity: Int) {

  /**
   * An array to hold objects, which will grow as needed.
   */
  private[this] var objects = new Array[Any](capacity)

  /**
   * How many objects in the pool.
   */
  private[this] var numObj: Int = 0

  /**
   * Returns how many objects in the pool.
   */
  def size(): Int = numObj

  /**
   * Returns the object at position `idx` in the array.
   */
  def get(idx: Int): Any = {
    require(idx < numObj)
    objects(idx)
  }

  /**
   * Puts an object `obj` at the end of array, returns the index of it.
   *
   * The array will grow as needed.
   */
  def put(obj: Any): Int = {
    if (numObj >= capacity) {
      capacity *= 2
      val tmp = new Array[Any](capacity)
      System.arraycopy(objects, 0, tmp, 0, objects.length)
      objects = tmp
    }
    objects(numObj) = obj
    val idx = numObj
    numObj += 1
    idx
  }

  /**
   * Replaces the object at `idx` with new one `obj`.
   */
  def replace(idx: Int, obj: Any): Unit = {
    require(idx < numObj)
    objects(idx) = obj
  }
}

/**
 * An unique object pool stores a collection of unique objects in it.
 *
 * @param capacity the initial capacity
 */
class UniqueObjectPool(capacity: Int) extends ObjectPool(capacity) {

  /**
   * A hash map from objects to their indexes in the array.
   */
  private[this] val objSet = new java.util.HashMap[Any, Int]()

  /**
   * Put an object `obj` into the pool. If there is an existing object equals to `obj`, it will
   * return the index of the existing one.
   */
  override def put(obj: Any): Int = {
    if (objSet.containsKey(obj)) {
      objSet.get(obj)
    } else {
      val idx = super.put(obj)
      objSet.put(obj, idx)
      idx
    }
  }

  /**
   * The objects can not be replaced.
   */
  override def replace(idx: Int, obj: Any): Unit = {
    throw new UnsupportedOperationException
  }
}
