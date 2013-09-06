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

package org.apache.spark.util


/**
 * A tuple of 2 elements. This can be used as an alternative to Scala's Tuple2 when we want to
 * minimize object allocation.
 *
 * @param  _1   Element 1 of this MutablePair
 * @param  _2   Element 2 of this MutablePair
 */
case class MutablePair[@specialized(Int, Long, Double, Char, Boolean/*, AnyRef*/) T1,
                      @specialized(Int, Long, Double, Char, Boolean/*, AnyRef*/) T2]
  (var _1: T1, var _2: T2)
  extends Product2[T1, T2]
{
  override def toString = "(" + _1 + "," + _2 + ")"

  override def canEqual(that: Any): Boolean = that.isInstanceOf[MutablePair[_,_]]
}
