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

import com.google.common.cache.CacheStats
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.FileMetaCacheManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class FileMetaCacheSuite extends QueryTest with SharedSparkSession
  with PrivateMethodTester {
  import testImplicits._

  private val cacheStatsMethod = PrivateMethod[CacheStats](Symbol("cacheStats"))
  private val cleanUpMethod = PrivateMethod[Unit](Symbol("cleanUp"))

  test("SPARK-36516: simple select queries with orc file meta cache") {
    withSQLConf(SQLConf.FILE_META_CACHE_ENABLED_SOURCE_LIST.key -> "orc") {
      val tableName = "orc_use_meta_cache"
      withTable(tableName) {
        (0 until 10).map(i => (i, i.toString)).toDF("id", "value")
          .write.format("orc").saveAsTable(tableName)
        try {
          val statsBeforeQuery = FileMetaCacheManager.invokePrivate(cacheStatsMethod())
          checkAnswer(sql(s"SELECT id FROM $tableName where id > 5"),
            (6 until 10).map(Row.apply(_)))
          val statsAfterQuery1 = FileMetaCacheManager.invokePrivate(cacheStatsMethod())
          // The 1st query triggers 4 times file meta read: 2 times related to
          // push down filter and 2 times related to file read. The 1st query
          // run twice: df.collect() and df.rdd.count(), so it triggers 8 times
          // file meta read in total. missCount is 2 because cache is empty and
          // 2 meta files need load, other 6 times will read meta from cache.
          assert(statsAfterQuery1.missCount() - statsBeforeQuery.missCount() == 2)
          assert(statsAfterQuery1.hitCount() - statsBeforeQuery.hitCount() == 6)
          checkAnswer(sql(s"SELECT id FROM $tableName where id < 5"),
            (0 until 5).map(Row.apply(_)))
          val statsAfterQuery2 = FileMetaCacheManager.invokePrivate(cacheStatsMethod())
          // The 2nd query also triggers 8 times file meta read in total and
          // all read from meta cache, so missCount no growth and hitCount
          // increase 8 times.
          assert(statsAfterQuery2.missCount() - statsAfterQuery1.missCount() == 0)
          assert(statsAfterQuery2.hitCount() - statsAfterQuery1.hitCount() == 8)
        } finally {
          FileMetaCacheManager.invokePrivate(cleanUpMethod())
        }
      }
    }
  }
}

class V1FileMetaCacheSuite extends FileMetaCacheSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "orc")
}

class V2FileMetaCacheSuite extends FileMetaCacheSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
