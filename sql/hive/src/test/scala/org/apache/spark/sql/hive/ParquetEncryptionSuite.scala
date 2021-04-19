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

package org.apache.spark.sql.hive

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.sys.process._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * A test suite that tests parquet modular encryption usage.
 */
class ParquetEncryptionSuite extends QueryTest with SharedSparkSession {

  private val encoder = Base64.getEncoder
  private val footerKey =
    encoder.encodeToString("0123456789012345".getBytes(StandardCharsets.UTF_8))
  private val key1 = encoder.encodeToString("1234567890123450".getBytes(StandardCharsets.UTF_8))
  private val key2 = encoder.encodeToString("1234567890123451".getBytes(StandardCharsets.UTF_8))

  import testImplicits._

  test("SPARK-34990: Write and read an encrypted parquet") {
    withTempDir { dir =>
      withSQLConf(
        "parquet.crypto.factory.class" ->
          "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory",
        "parquet.encryption.kms.client.class" -> "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        "parquet.encryption.key.list" ->
          s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

        val inputDF = Seq((1, 22, 333)).toDF("a", "b", "c")
        val parquetDir = new File(dir, "parquet").getCanonicalPath
        inputDF.write
          .option("parquet.encryption.column.keys", "key1: a, b; key2: c")
          .option("parquet.encryption.footer.key", "footerKey")
          .parquet(parquetDir)

        verifyParquetEncrypted(parquetDir)

        val parquetDF = spark.read.parquet(parquetDir)
        assert(parquetDF.inputFiles.nonEmpty)
        val readDataset = parquetDF.select("a", "b", "c")
        checkAnswer(readDataset, inputDF)
      }
    }
  }

  private def verifyParquetEncrypted(parquetDir: String) = {
    val parquetPartitionFile = Seq("ls", "-tr", parquetDir).!!.split("\\s+")(0)
    val fullFilename = parquetDir + "/" + parquetPartitionFile
    val magic = Seq("tail", "-c", "4", fullFilename).!!
    assert(magic.stripLineEnd.trim() == "PARE")
  }
}
