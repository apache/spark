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

package org.apache.spark.serializer

import java.io.ByteArrayOutputStream

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper

class SerializerManagerSuite extends SparkFunSuite {

  test("wrapForChecksum folds a deterministic content checksum over the written bytes") {
    val sparkConf = new SparkConf(false)
    val serManager = new SerializerManager(new JavaSerializer(sparkConf), sparkConf)
    def checksumOf(bytes: Array[Byte]): Long = {
      val checksum = ShuffleChecksumHelper.getChecksumByAlgorithm("CRC32C")
      val out = serManager.wrapForChecksum(checksum, new ByteArrayOutputStream())
      out.write(bytes)
      out.close()
      checksum.getValue
    }
    val a = Array[Byte](1, 2, 3, 4, 5)
    val b = Array[Byte](1, 2, 3, 4, 5)
    val c = Array[Byte](1, 2, 3, 4, 6)
    // Identical content gives an identical checksum (so equal replicas survive a seal); divergent
    // content gives a different one (so a divergently-materialized copy is detectable).
    assert(checksumOf(a) === checksumOf(b))
    assert(checksumOf(a) !== checksumOf(c))
    assert(checksumOf(Array.empty[Byte]) === checksumOf(Array.empty[Byte]))
  }
}
