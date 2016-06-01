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

package org.apache.spark.sql.execution.streaming

/**
 * An offset is a monotonically increasing metric used to track progress in the computation of a
 * stream. An [[Offset]] must be comparable, and the result of `compareTo` must be consistent
 * with `equals` and `hashcode`.
 */
trait Offset extends Serializable {

  /**
   * Returns a negative integer, zero, or a positive integer as this object is less than, equal to,
   * or greater than the specified object.
   */
  def compareTo(other: Offset): Int

  def >(other: Offset): Boolean = compareTo(other) > 0
  def <(other: Offset): Boolean = compareTo(other) < 0
  def <=(other: Offset): Boolean = compareTo(other) <= 0
  def >=(other: Offset): Boolean = compareTo(other) >= 0
}
