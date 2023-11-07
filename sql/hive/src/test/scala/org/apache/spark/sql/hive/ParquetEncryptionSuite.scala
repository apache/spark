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
import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton

/**
 * A test suite that tests parquet modular encryption usage.
 */
class ParquetEncryptionSuite extends QueryTest with TestHiveSingleton {
  import spark.implicits._

  private val encoder = Base64.getEncoder
  private val footerKey =
    encoder.encodeToString("0123456789012345".getBytes(StandardCharsets.UTF_8))
  private val key1 = encoder.encodeToString("1234567890123450".getBytes(StandardCharsets.UTF_8))
  private val key2 = encoder.encodeToString("1234567890123451".getBytes(StandardCharsets.UTF_8))

  test("SPARK-34990: Write and read an encrypted parquet") {
    withTempDir { dir =>
      withSQLConf(
        "parquet.crypto.factory.class" ->
          "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory",
        "parquet.encryption.kms.client.class" ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
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

  test("SPARK-37117: Can't read files in Parquet encryption external key material mode") {
    withTempDir { dir =>
      withSQLConf(
        "parquet.crypto.factory.class" ->
          "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory",
        "parquet.encryption.kms.client.class" ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        "parquet.encryption.key.material.store.internally" ->
          "false",
        "parquet.encryption.key.list" ->
          s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

        val inputDF = Seq((1, 22, 333)).toDF("a", "b", "c")
        val parquetDir = new File(dir, "parquet").getCanonicalPath
        inputDF.write
          .option("parquet.encryption.column.keys", "key1: a, b; key2: c")
          .option("parquet.encryption.footer.key", "footerKey")
          .parquet(parquetDir)

        val parquetDF = spark.read.parquet(parquetDir)
        assert(parquetDF.inputFiles.nonEmpty)
        val readDataset = parquetDF.select("a", "b", "c")
        checkAnswer(readDataset, inputDF)
      }
    }
  }

  test("SPARK-42114: Test of uniform parquet encryption") {
    withTempDir { dir =>
      withSQLConf(
        "parquet.crypto.factory.class" ->
          "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory",
        "parquet.encryption.kms.client.class" ->
          "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
        "parquet.encryption.key.list" ->
          s"key1: ${key1}") {

        val inputDF = Seq((1, 22, 333)).toDF("a", "b", "c")
        val parquetDir = new File(dir, "parquet").getCanonicalPath
        inputDF.write
          .option("parquet.encryption.uniform.key", "key1")
          .parquet(parquetDir)

        verifyParquetEncrypted(parquetDir)

        val parquetDF = spark.read.parquet(parquetDir)
        assert(parquetDF.inputFiles.nonEmpty)
        val readDataset = parquetDF.select("a", "b", "c")
        checkAnswer(readDataset, inputDF)
      }
    }
  }

  /**
   * Verify that the directory contains an encrypted parquet in
   * encrypted footer mode by means of checking for all the parquet part files
   * in the parquet directory that their magic string is PARE, as defined in the spec:
   * https://github.com/apache/parquet-format/blob/master/Encryption.md#54-encrypted-footer-mode
   */
  private def verifyParquetEncrypted(parquetDir: String): Unit = {
    val parquetPartitionFiles = getListOfParquetFiles(new File(parquetDir))
    assert(parquetPartitionFiles.size >= 1)
    parquetPartitionFiles.foreach { parquetFile =>
      val magicString = "PARE"
      val magicStringLength = magicString.length()
      val byteArray = new Array[Byte](magicStringLength)
      val randomAccessFile = new RandomAccessFile(parquetFile, "r")
      try {
        randomAccessFile.read(byteArray, 0, magicStringLength)
      } finally {
        randomAccessFile.close()
      }
      val stringRead = new String(byteArray, StandardCharsets.UTF_8)
      assert(magicString == stringRead)
    }
  }

  private def getListOfParquetFiles(dir: File): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      file.getName.endsWith("parquet")
    }
  }
}
