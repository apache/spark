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
package org.apache.spark.sql.execution.datasources.xml

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession

/**
 * Tests various cases of partition size, compression.
 */
final class XmlPartitioningSuite extends SparkFunSuite with Matchers with BeforeAndAfterAll {

  private def doPartitionTest(suffix: String, blockSize: Long, large: Boolean): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("XmlPartitioningSuite")
      .config("spark.hadoop.fs.local.block.size", blockSize)
      .getOrCreate()
    try {
      val fileName = s"test-data/xml-resources/fias_house${if (large) ".large" else ""}.xml$suffix"
      val xmlFile = getClass.getClassLoader.getResource(fileName).getFile
      val results = spark.read.option("rowTag", "House").option("mode", "FAILFAST").xml(xmlFile)
      // Test file has 37 records; large file is 20x the records
      assert(results.count() === (if (large) 740 else 37))
    } finally {
      spark.stop()
    }
  }

  test("Uncompressed small file with specially chosen block size") {
    doPartitionTest("", 8342, false)
  }

  test("Uncompressed small file with small block size") {
    doPartitionTest("", 500, false)
  }

  test("bzip2 small file with small block size") {
    doPartitionTest(".bz2", 500, false)
  }

  test("bzip2 large file with small block size") {
    // Note, the large bzip2 test file was compressed such that there are several blocks
    // in the compressed input (e.g. bzip2 -1 on a file with much more than 100k data)
    doPartitionTest(".bz2", 500, true)
  }

  test("gzip small file") {
    // Block size won't matter
    doPartitionTest(".gz", 500, false)
  }

  test("gzip large file") {
    // Block size won't matter
    doPartitionTest(".gz", 500, true)
  }

}
