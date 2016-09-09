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

import org.apache.spark.SparkFunSuite

class FileStreamSourceSuite extends SparkFunSuite {

  import FileStreamSource._

  test("SeenFilesMap") {
    val map = new SeenFilesMap(maxAgeMs = 10)

    map.add(FileEntry("a", 5))
    assert(map.size == 1)
    map.purge()
    assert(map.size == 1)

    // Add a new entry and purge should be no-op, since the gap is exactly 10 ms.
    map.add(FileEntry("b", 15))
    assert(map.size == 2)
    map.purge()
    assert(map.size == 2)

    // Add a new entry that's more than 10 ms than the first entry. We should be able to purge now.
    map.add(FileEntry("c", 16))
    assert(map.size == 3)
    map.purge()
    assert(map.size == 2)

    // Override existing entry shouldn't change the size
    map.add(FileEntry("c", 25))
    assert(map.size == 2)

    // Not a new file because we have seen c before
    assert(!map.isNewFile(FileEntry("c", 20)))

    // Not a new file because timestamp is too old
    assert(!map.isNewFile(FileEntry("d", 5)))

    // Finally a new file: never seen and not too old
    assert(map.isNewFile(FileEntry("e", 20)))
  }

  test("SeenFilesMap should only consider a file old if it is earlier than last purge time") {
    val map = new SeenFilesMap(maxAgeMs = 10)

    map.add(FileEntry("a", 20))
    assert(map.size == 1)

    // Timestamp 5 should still considered a new file because purge time should be 0
    assert(map.isNewFile(FileEntry("b", 9)))
    assert(map.isNewFile(FileEntry("b", 10)))

    // Once purge, purge time should be 10 and then b would be a old file if it is less than 10.
    map.purge()
    assert(!map.isNewFile(FileEntry("b", 9)))
    assert(map.isNewFile(FileEntry("b", 10)))
  }

}
