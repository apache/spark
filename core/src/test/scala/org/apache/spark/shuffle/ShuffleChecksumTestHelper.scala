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

package org.apache.spark.shuffle

import java.io.File

trait ShuffleChecksumTestHelper {

  /**
   * Ensure that the checksum values are consistent between write and read side.
   */
  def compareChecksums(
      numPartition: Int,
      algorithm: String,
      checksum: File,
      data: File,
      index: File): Unit = {
    assert(checksum.exists(), "Checksum file doesn't exist")
    assert(data.exists(), "Data file doesn't exist")
    assert(index.exists(), "Index file doesn't exist")

    assert(ShuffleChecksumUtils.compareChecksums(numPartition, algorithm, checksum, data, index),
      "checksum must be consistent at both write and read sides")
  }
}
