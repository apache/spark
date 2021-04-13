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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.{Base64, HashMap, Map}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.crypto.{KeyAccessDeniedException, ParquetCryptoRuntimeException}
import org.apache.parquet.crypto.keytools.{KeyToolkit, KmsClient}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

import scala.sys.process._

/**
 * A test suite that tests parquet modular encryption usage in Spark.
 */
class ParquetEncryptionTest extends QueryTest with SharedSparkSession {

  private val encoder = Base64.getEncoder
  private val footerKey =
    encoder.encodeToString("0123456789012345".getBytes(StandardCharsets.UTF_8))
  private val key1 = encoder.encodeToString("1234567890123450".getBytes(StandardCharsets.UTF_8))
  private val key2 = encoder.encodeToString("1234567890123451".getBytes(StandardCharsets.UTF_8))

  import testImplicits._

  test("Write and read an encrypted parquet") {
    withTempDir { dir =>
      spark.conf.set(
        "parquet.crypto.factory.class",
        "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
      spark.conf.set(
        "parquet.encryption.kms.client.class",
        "org.apache.spark.sql.execution.datasources.parquet.InMemoryKMS")
      spark.conf.set(
        "parquet.encryption.key.list",
        s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}")

      val df = Seq((1, 22, 333)).toDF("a", "b", "c")
      val parquetDir = new File(dir, "parquet").getCanonicalPath
      df.write
        .option("parquet.encryption.column.keys", "key1: a, b; key2: c")
        .option("parquet.encryption.footer.key", "footerKey")
        .parquet(parquetDir)

      val parquetPartitionFile = Seq("ls", "-tr", parquetDir).!!.split("\\s+")(0)
      val fullFilename = parquetDir + "/" + parquetPartitionFile
      val magic = Seq("tail", "-c", "4", fullFilename).!!
      assert(magic.stripLineEnd.trim() == "PARE")

      val parquetDF = spark.read.parquet(parquetDir)
      assert(parquetDF.inputFiles.nonEmpty)
      val ds = parquetDF.select("a", "b", "c")
      ds.show()
    }
  }
}

/**
 * This is a mock class, built just for parquet encryption testing in Spark
 * and based on InMemoryKMS in parquet-hadoop tests.
 * Don't use it as an example of a KmsClient implementation.
 * Use parquet-hadoop/src/test/java/org/apache/parquet/crypto/keytools/samples/VaultClient.java
 * as a sample implementation instead.
 */
class InMemoryKMS extends KmsClient {
  private var masterKeyMap: Map[String, Array[Byte]] = null

  override def initialize(
      configuration: Configuration,
      kmsInstanceID: String,
      kmsInstanceURL: String,
      accessToken: String) = { // Parse master  keys
    val masterKeys: Array[String] =
      configuration.getTrimmedStrings(InMemoryKMS.KEY_LIST_PROPERTY_NAME)
    if (null == masterKeys || masterKeys.length == 0) {
      throw new ParquetCryptoRuntimeException("No encryption key list")
    }
    masterKeyMap = InMemoryKMS.parseKeyList(masterKeys)
  }

  @throws[KeyAccessDeniedException]
  @throws[UnsupportedOperationException]
  override def wrapKey(keyBytes: Array[Byte], masterKeyIdentifier: String): String = {
    println(s"Wrap Key ${masterKeyIdentifier}")
    // Always use the latest key version for writing
    val masterKey = masterKeyMap.get(masterKeyIdentifier)
    if (null == masterKey) {
      throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier)
    }
    KeyToolkit.encryptKeyLocally(keyBytes, masterKey, null /*AAD*/ )
  }

  @throws[KeyAccessDeniedException]
  @throws[UnsupportedOperationException]
  override def unwrapKey(wrappedKey: String, masterKeyIdentifier: String): Array[Byte] = {
    println(s"Unwrap Key ${masterKeyIdentifier}")
    val masterKey: Array[Byte] = masterKeyMap.get(masterKeyIdentifier)
    if (null == masterKey) {
      throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier)
    }
    KeyToolkit.decryptKeyLocally(wrappedKey, masterKey, null /*AAD*/ )
  }
}

object InMemoryKMS {
  val KEY_LIST_PROPERTY_NAME: String = "parquet.encryption.key.list"

  private def parseKeyList(masterKeys: Array[String]): Map[String, Array[Byte]] = {
    val keyMap: Map[String, Array[Byte]] = new HashMap[String, Array[Byte]]
    val nKeys: Int = masterKeys.length
    for (i <- 0 until nKeys) {
      val parts: Array[String] = masterKeys(i).split(":")
      val keyName: String = parts(0).trim
      if (parts.length != 2) {
        throw new IllegalArgumentException("Key '" + keyName + "' is not formatted correctly")
      }
      val key: String = parts(1).trim
      try {
        val keyBytes: Array[Byte] = Base64.getDecoder.decode(key)
        keyMap.put(keyName, keyBytes)
      } catch {
        case e: IllegalArgumentException =>
          throw e
      }
    }
    keyMap
  }
}
