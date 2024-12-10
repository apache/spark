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

package org.apache.spark.ml.recommendation.logfac.local

import scala.reflect.ClassTag

case class PrimitiveArrayBuffer[@specialized(Long, Float) T: ClassTag](private var _arr: Array[T],
                                                                       private var _n: Int) {

  private def ensureCapacity(n: Int): Unit = {
    _arr = if (_arr.length < n) {
      val arrNew = new Array[T](_arr.length + (_arr.length >> 1))
      System.arraycopy(_arr, 0, arrNew, 0, _arr.length)
      arrNew
    } else {
      _arr
    }
  }

  def clear(): Unit = {
    _n = 0
  }

  def toArray: Array[T] = {
    val result = new Array[T](_n)
    System.arraycopy(_arr, 0, result, 0, _n)
    result
  }

  def add(elem: T): Unit = {
    ensureCapacity(_n + 1)
    _arr(_n) = elem
    _n += 1
  }

  def get(i: Int): T = {
    _arr(i)
  }

  def size: Int = _n
}

object PrimitiveArrayBuffer {
  def empty[@specialized(Long, Float) T: ClassTag]: PrimitiveArrayBuffer[T] =
    PrimitiveArrayBuffer(new Array[T](64), 0)
}
