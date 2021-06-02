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

package org.apache.spark.sql.execution.streaming.state

import java.io._
import java.nio.charset.Charset

import org.apache.commons.io.FileUtils

import org.apache.spark._

class RocksDBSuite extends SparkFunSuite {

  test("checkpoint metadata serde roundtrip") {
    def checkJsonRoundtrip(metadata: RocksDBCheckpointMetadata, json: String): Unit = {
      assert(metadata.json == json)
      withTempDir { dir =>
        val file = new File(dir, "json")
        FileUtils.write(file, s"v1\n$json", Charset.defaultCharset)
        assert(metadata == RocksDBCheckpointMetadata.readFromFile(file))
      }
    }
    val sstFiles = Seq(RocksDBSstFile("00001.sst", "00001-uuid.sst", 12345678901234L))
    val logFiles = Seq(RocksDBLogFile("00001.log", "00001-uuid.log", 12345678901234L))

    // scalastyle:off line.size.limit
    // should always include sstFiles and numKeys
    checkJsonRoundtrip(
      RocksDBCheckpointMetadata(Seq.empty, 0L),
      """{"sstFiles":[],"numKeys":0}"""
    )
    // shouldn't include the "logFiles" field in json when it's empty
    checkJsonRoundtrip(
      RocksDBCheckpointMetadata(sstFiles, 12345678901234L),
      """{"sstFiles":[{"localFileName":"00001.sst","dfsSstFileName":"00001-uuid.sst","sizeBytes":12345678901234}],"numKeys":12345678901234}"""
    )
    checkJsonRoundtrip(
      RocksDBCheckpointMetadata(sstFiles, logFiles, 12345678901234L),
      """{"sstFiles":[{"localFileName":"00001.sst","dfsSstFileName":"00001-uuid.sst","sizeBytes":12345678901234}],"logFiles":[{"localFileName":"00001.log","dfsLogFileName":"00001-uuid.log","sizeBytes":12345678901234}],"numKeys":12345678901234}""")
    // scalastyle:on line.size.limit
  }
}
