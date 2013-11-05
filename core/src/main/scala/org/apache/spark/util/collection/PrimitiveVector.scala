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

package org.apache.spark.util.collection

/** Provides a simple, non-threadsafe, array-backed vector that can store primitives. */
private[spark]
class PrimitiveVector[@specialized(Long, Int, Double) V: ClassManifest](initialSize: Int = 64) {
  private var numElements = 0
  private var array: Array[V] = _

  // NB: This must be separate from the declaration, otherwise the specialized parent class
  // will get its own array with the same initial size. TODO: Figure out why...
  array = new Array[V](initialSize)

  def apply(index: Int): V = {
    require(index < numElements)
    array(index)
  }

  def +=(value: V) {
    if (numElements == array.length) { resize(array.length * 2) }
    array(numElements) = value
    numElements += 1
  }

  def length = numElements

  def getUnderlyingArray = array

  /** Resizes the array, dropping elements if the total length decreases. */
  def resize(newLength: Int) {
    val newArray = new Array[V](newLength)
    array.copyToArray(newArray)
    array = newArray
  }
}
