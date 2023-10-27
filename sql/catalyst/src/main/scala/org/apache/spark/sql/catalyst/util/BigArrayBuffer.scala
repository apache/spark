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

package org.apache.spark.sql.catalyst.util

import scala.collection.generic.{CanBuildFrom, GenericCompanion, GenericTraversableTemplate, SeqFactory}
import scala.collection.mutable.Builder

// https://github.com/scala/scala/pull/9819
// There are bugs in ArraryBuffer in scala-2.12 during resizeUp.
// This bug was fixed in 2.13
class BigArrayBuffer[A] extends scala.collection.mutable.ArrayBuffer[A]
  with Builder[A, BigArrayBuffer[A]]
  with GenericTraversableTemplate[A, BigArrayBuffer] {

  // TODO 3.T: should be `protected`, perhaps `protected[this]`
  /** Ensure that the internal array has at least `n` additional cells more than `size0`. */
  private def ensureAdditionalSize(n: Int): Unit = {
    // `.toLong` to ensure `Long` arithmetic is used and prevent `Int` overflow
    array = BigArrayBuffer.ensureSize(array, size0, size0.toLong + n)
  }

  override def +=(elem: A): this.type = {
    ensureAdditionalSize(1)
    array(size0) = elem.asInstanceOf[AnyRef]
    size0 += 1
    this
  }


  // Explicitly override the `result` method and specify its return type
  override def result(): BigArrayBuffer[A] = this

  // Explicitly override the companion method and specify its return type
  override def companion: GenericCompanion[BigArrayBuffer] = BigArrayBuffer

}

object BigArrayBuffer extends SeqFactory[BigArrayBuffer] {
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, BigArrayBuffer[A]] =
    ReusableCBF.asInstanceOf[GenericCanBuildFrom[A]]
  def newBuilder[A]: Builder[A, BigArrayBuffer[A]] = new BigArrayBuffer[A]

  final val DefaultInitialSize = 16
  final val VM_MaxArraySize = Int.MaxValue - 8

  override def empty[A]: BigArrayBuffer[A] = new BigArrayBuffer[A]()

  /**
   * @param arrayLen  the length of the backing array
   * @param targetLen the minimum length to resize up to
   * @return -1 if no resizing is needed, or the size for the new array otherwise
   */
  private def resizeUp(arrayLen: Long, targetLen: Long): Int = {
    if (targetLen <= arrayLen) -1
    else {
      if (targetLen > Int.MaxValue) {
        throw new Exception(s"Collections cannot have more than ${Int.MaxValue} elements")
      }
      if (targetLen.toInt > VM_MaxArraySize) {
        throw new Exception(s"Size of array-backed collection exceeds " +
          s"VM array size limit of ${VM_MaxArraySize}")
      }
      val newLen = math.max(targetLen, math.max(arrayLen * 2, DefaultInitialSize))
      val resizeLen = math.min(newLen, VM_MaxArraySize).toInt
      resizeLen
    }
  }

  // if necessary, copy (curSize elements of) the array to a new array of capacity n.
  // Should use Array.copyOf(array, resizeEnsuring(array.length))?
  private def ensureSize(array: Array[AnyRef], curSize: Int, targetSize: Long): Array[AnyRef] = {
    val arrNewLen = resizeUp(array.length, targetSize)
    if (arrNewLen < 0) array
    else {
      val res = new Array[AnyRef](arrNewLen)
      System.arraycopy(array, 0, res, 0, curSize)
      res
    }
  }
}

