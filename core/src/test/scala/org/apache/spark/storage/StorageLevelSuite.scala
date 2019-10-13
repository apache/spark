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

package org.apache.spark.storage

import org.apache.spark.SparkFunSuite


class StorageLevelSuite extends SparkFunSuite{


  private def testFromDescription(oldLevel: StorageLevel): Unit = {
    val desc = oldLevel.description
    val newLevel = StorageLevel.fromDescription(desc)
    assert(oldLevel === newLevel)
  }

  test("from description") {
    testFromDescription(StorageLevel.NONE)
    testFromDescription(StorageLevel.DISK_ONLY)
    testFromDescription(StorageLevel.DISK_ONLY_2)
    testFromDescription(StorageLevel.MEMORY_ONLY)
    testFromDescription(StorageLevel.MEMORY_ONLY_2)
    testFromDescription(StorageLevel.MEMORY_ONLY_SER)
    testFromDescription(StorageLevel.MEMORY_ONLY_SER_2)
    testFromDescription(StorageLevel.MEMORY_AND_DISK)
    testFromDescription(StorageLevel.MEMORY_AND_DISK_2)
    testFromDescription(StorageLevel.MEMORY_AND_DISK_SER)
    testFromDescription(StorageLevel.MEMORY_AND_DISK_SER_2)
    testFromDescription(StorageLevel.OFF_HEAP)
  }
}
