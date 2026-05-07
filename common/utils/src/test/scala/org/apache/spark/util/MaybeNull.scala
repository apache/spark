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

/* The MaybeNull class is a utility that introduces controlled nullability into a sequence
 * of invocations. It is designed to return a ~null~ value at a specified interval while returning
 * the provided value for all other invocations.
 */
case class MaybeNull(interval: Int) {
  assert(interval > 1)
  private var invocations = 0
  def apply[T](value: T): T = {
    val result = if (invocations % interval == 0) {
      null.asInstanceOf[T]
    } else {
      value
    }
    invocations += 1
    result
  }
}
