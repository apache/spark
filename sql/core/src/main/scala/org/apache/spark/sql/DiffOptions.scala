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

package org.apache.spark.sql

case class DiffOptions(diffColumn: String,
                       leftColumnPrefix: String,
                       rightColumnPrefix: String,
                       insertDiffValue: String,
                       changeDiffValue: String,
                       deleteDiffValue: String,
                       nochangeDiffValue: String) {

  require(diffColumn.nonEmpty, "Diff column name must not be empty")

  require(leftColumnPrefix.nonEmpty, "Left column prefix must not be empty")
  require(rightColumnPrefix.nonEmpty, "Right column prefix must not be empty")
  require(leftColumnPrefix != rightColumnPrefix,
    s"Left and right column prefix must be distinct: $leftColumnPrefix")

  require(insertDiffValue.nonEmpty, "Insert diff value must not be empty")
  require(changeDiffValue.nonEmpty, "Change diff value must not be empty")
  require(deleteDiffValue.nonEmpty, "Delete diff value must not be empty")
  require(nochangeDiffValue.nonEmpty, "No-change diff value must not be empty")

  val diffValues = Seq(insertDiffValue, changeDiffValue, deleteDiffValue, nochangeDiffValue)
  require(diffValues.distinct.length == diffValues.length,
    s"Diff values must be distinct: $diffValues")
}

object DiffOptions {
  /**
   * Default diffing options.
   */
  val default = DiffOptions("diff", "left", "right", "I", "C", "D", "N")
}
