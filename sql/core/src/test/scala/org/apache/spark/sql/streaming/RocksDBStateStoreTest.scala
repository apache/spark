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

package org.apache.spark.sql.streaming

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.execution.streaming.state.{RocksDBConf, RocksDBStateStoreProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
trait RocksDBStateStoreTest extends SQLTestUtils {

  val rocksdbChangelogCheckpointingConfKey: String = RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX +
    ".changelogCheckpointing.enabled"

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
                             (implicit pos: Position): Unit = {
    super.test(testName + " (RocksDBStateStore)", testTags: _*) {
      withSQLConf(rocksdbChangelogCheckpointingConfKey -> "false",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
        testBody
      }
      // in case tests have any code that needs to execute after every test
      super.afterEach()
    }

    super.test(testName + " (RocksDBStateStore with changelog checkpointing)", testTags: _*) {
      // in case tests have any code that needs to execute before every test
      super.beforeEach()
      withSQLConf(rocksdbChangelogCheckpointingConfKey -> "true",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
        testBody
      }
    }
  }
}
