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

package org.apache.spark.sql.catalyst.trees

import org.apache.spark.sql.catalyst.trees.TreePattern.TreePattern
import org.apache.spark.util.collection.BitSet

// A wrapper of BitSet for pattern enums.
trait TreePatternBits {
  protected val treePatternBits: BitSet

  /**
   * @param t, the tree pattern enum to be tested.
   * @return true if the bit for `t` is set; false otherwise.
   */
  @inline final def containsPattern(t: TreePattern): Boolean = {
    treePatternBits.get(t.id)
  }

  /**
   * @param patterns, a sequence of tree pattern enums to be tested.
   * @return true if every bit for `patterns` is set; false otherwise.
   */
  final def containsAllPatterns(patterns: TreePattern*): Boolean = {
    val iterator = patterns.iterator
    while (iterator.hasNext) {
      if (!containsPattern(iterator.next())) {
        return false
      }
    }
    true
  }

  /**
   * @param patterns, a sequence of tree pattern enums to be tested.
   * @return true if at least one bit for `patterns` is set; false otherwise.
   */
  final def containsAnyPattern(patterns: TreePattern*): Boolean = {
    val iterator = patterns.iterator
    while (iterator.hasNext) {
      if (containsPattern(iterator.next())) {
        return true
      }
    }
    false
  }
}
