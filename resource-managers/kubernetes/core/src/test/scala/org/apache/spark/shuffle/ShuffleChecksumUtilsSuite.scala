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

import java.io.{DataOutputStream, File, FileOutputStream}

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper.getChecksumFileName
import org.apache.spark.shuffle.KubernetesLocalDiskShuffleExecutorComponents.verifyChecksum
import org.apache.spark.storage.{ShuffleChecksumBlockId, ShuffleDataBlockId, ShuffleIndexBlockId}


class ShuffleChecksumUtilsSuite extends SparkFunSuite {

  private val ALGORITHM = "ADLER32"
  private val NON_EXIST_FILE = new File("non-exist-data-file")
  private val dataBlockId = ShuffleDataBlockId(0, 0, 0)
  private val indexBlockId = ShuffleIndexBlockId(0, 0, 0)
  private val checksumBlockId = ShuffleChecksumBlockId(0, 0, 0)

  test("verifyChecksum fails if dataFile doesnt' exist") {
    assert(!verifyChecksum(ALGORITHM, dataBlockId, null, null, null))
    assert(!verifyChecksum(ALGORITHM, dataBlockId, null, null, NON_EXIST_FILE))
  }

  test("verifyChecksum ignores shuffle index files") {
    withTempDir { dir =>
      val dataFile = new File(dir, dataBlockId.name)
      dataFile.createNewFile()
      assert(verifyChecksum(ALGORITHM, indexBlockId, null, null, dataFile))
    }
  }

  test("verifyChecksum succeeds if a checksum file doesn't exist") {
    withTempDir { dir =>
      val dataFile = new File(dir, dataBlockId.name)
      dataFile.createNewFile()
      assert(verifyChecksum(ALGORITHM, dataBlockId, null, null, dataFile))
      assert(verifyChecksum(ALGORITHM, dataBlockId, NON_EXIST_FILE, null, dataFile))
    }
  }

  test("verifyChecksum fails if a checksum file is empty or corrupted") {
    withTempDir { dir =>
      val dataFile = new File(dir, dataBlockId.name)
      dataFile.createNewFile()

      val checksumFileName = getChecksumFileName(checksumBlockId.name, ALGORITHM)
      val checksumFile = new File(dir, checksumFileName)
      checksumFile.createNewFile()
      assert(!verifyChecksum(ALGORITHM, dataBlockId, checksumFile, null, dataFile))

      val out = new DataOutputStream(new FileOutputStream(checksumFile))
      out.writeInt(1)
      out.close()
      assert(!verifyChecksum(ALGORITHM, dataBlockId, checksumFile, null, dataFile))
    }
  }

  test("verifyChecksum fails if an index file is missing.") {
    withTempDir { dir =>
      val dataFile = new File(dir, dataBlockId.name)
      dataFile.createNewFile()

      val checksumFileName = getChecksumFileName(checksumBlockId.name, ALGORITHM)
      val checksumFile = new File(dir, checksumFileName)
      checksumFile.createNewFile()
      val out = new DataOutputStream(new FileOutputStream(checksumFile))
      out.writeLong(0)
      out.close()

      assert(!verifyChecksum(ALGORITHM, dataBlockId, checksumFile, null, dataFile))
      assert(!verifyChecksum(ALGORITHM, dataBlockId, checksumFile, NON_EXIST_FILE, dataFile))
    }
  }

  test("verifyChecksum fails if an index file is empty.") {
    withTempDir { dir =>
      val dataFile = new File(dir, dataBlockId.name)
      dataFile.createNewFile()

      val checksumFileName = getChecksumFileName(checksumBlockId.name, ALGORITHM)
      val checksumFile = new File(dir, checksumFileName)
      checksumFile.createNewFile()
      val out = new DataOutputStream(new FileOutputStream(checksumFile))
      out.writeLong(0)
      out.close()

      val indexFile = new File(dir, indexBlockId.name)
      indexFile.createNewFile()

      assert(!verifyChecksum(ALGORITHM, dataBlockId, checksumFile, indexFile, dataFile))
    }
  }

  test("verifyChecksum succeeds") {
    withTempDir { dir =>
      val indexFile = new File(dir, indexBlockId.name)
      val dataFile = new File(dir, dataBlockId.name)
      val checksumFileName = getChecksumFileName(checksumBlockId.name, ALGORITHM)
      val checksumFile = new File(dir, checksumFileName)

      indexFile.createNewFile()
      dataFile.createNewFile()
      checksumFile.createNewFile()

      val dos = new DataOutputStream(new FileOutputStream(indexFile))
      dos.writeLong(0) // previous offset
      dos.writeLong(0) // current offset
      dos.close()

      val out = new DataOutputStream(new FileOutputStream(checksumFile))
      out.writeLong(1) // Checksum for empty data file
      out.close()

      assert(verifyChecksum(ALGORITHM, dataBlockId, checksumFile, indexFile, dataFile))
    }
  }
}
