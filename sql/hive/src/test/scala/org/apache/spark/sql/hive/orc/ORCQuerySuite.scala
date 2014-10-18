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

package org.apache.spark.sql.hive.orc

import java.util.Properties
import org.scalatest.BeforeAndAfterAll
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.catalyst.util.getTempFilePath
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.util.Utils
import java.io.File

case class TestRDDEntry(key: Int, value: String)

case class AllDataTypes(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean)

class OrcQuerySuite extends QueryTest with BeforeAndAfterAll {

  test("Read/Write All Types") {
    val tempDir = getTempFilePath("orcTest").getCanonicalPath
    val range = (0 to 255)
    val data = sparkContext.parallelize(range)
      .map(x => AllDataTypes(s"$x", x, x.toLong, x.toFloat, x.toDouble, x.toShort, x.toByte, x % 2 == 0))

    data.saveAsOrcFile(tempDir)
    checkAnswer(
      TestHive.orcFile(tempDir),
      data.toSchemaRDD.collect().toSeq)

    Utils.deleteRecursively(new File(tempDir))
  }

  test("Compression options for writing to a Orcfile") {
    val tempDir = getTempFilePath("orcTest").getCanonicalPath
    val rdd = TestHive.sparkContext.parallelize((1 to 100))
      .map(i => TestRDDEntry(i, s"val_$i"))

    // test default compression codec, now only support zlib
    rdd.saveAsOrcFile(tempDir)
    val actualCodec = OrcFileOperator.getMetaDataReader(new Path(tempDir), Some(new Configuration())).getCompression.name
    assert(actualCodec == "ZLIB")

    Utils.deleteRecursively(new File(tempDir))
  }

  test("Get ORC Schema with ORC Reader") {
    val path = "sql/hive/src/test/resources/data/files/orcfiles"
    val attributes = OrcFileOperator.orcSchema(path, Some(TestHive.sparkContext.hadoopConfiguration), new Properties())
    assert(attributes(0).dataType == StringType)
    assert(attributes(1).dataType == IntegerType)
    assert(attributes(2).dataType == LongType)
    assert(attributes(3).dataType == FloatType)
    assert(attributes(4).dataType == DoubleType)
    assert(attributes(5).dataType == ShortType)
    assert(attributes(6).dataType == ByteType)
    assert(attributes(7).dataType == BooleanType)
  }
}
