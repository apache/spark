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
package org.apache.spark.deploy.kubernetes.submit.v2

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.Secret
import scala.collection.JavaConverters._
import scala.collection.Map

import org.apache.spark.{SparkFunSuite, SSLOptions}
import org.apache.spark.util.Utils

class SubmittedDependencySecretBuilderSuite extends SparkFunSuite {

  private val SECRET_NAME = "submitted-dependency-secret"
  private val JARS_SECRET = "jars-secret"
  private val FILES_SECRET = "files-secret"
  private val JARS_SECRET_KEY = "jars-secret-key"
  private val FILES_SECRET_KEY = "files-secret-key"
  private val TRUSTSTORE_SECRET_KEY = "truststore-secret-key"
  private val TRUSTSTORE_STRING_CONTENTS = "trustStore-contents"

  test("Building the secret without a trustStore") {
    val builder = new SubmittedDependencySecretBuilderImpl(
      SECRET_NAME,
      JARS_SECRET,
      FILES_SECRET,
      JARS_SECRET_KEY,
      FILES_SECRET_KEY,
      TRUSTSTORE_SECRET_KEY,
      SSLOptions())
    val secret = builder.build()
    assert(secret.getMetadata.getName === SECRET_NAME)
    val secretDecodedData = decodeSecretData(secret)
    val expectedSecretData = Map(JARS_SECRET_KEY -> JARS_SECRET, FILES_SECRET_KEY -> FILES_SECRET)
    assert(secretDecodedData === expectedSecretData)
  }

  private def decodeSecretData(secret: Secret): Map[String, String] = {
    val secretData = secret.getData.asScala
    secretData.mapValues(encoded =>
      new String(BaseEncoding.base64().decode(encoded), Charsets.UTF_8))
  }

  test("Building the secret with a trustStore") {
    val tempTrustStoreDir = Utils.createTempDir(namePrefix = "temp-truststores")
    try {
      val trustStoreFile = new File(tempTrustStoreDir, "trustStore.jks")
      Files.write(TRUSTSTORE_STRING_CONTENTS, trustStoreFile, Charsets.UTF_8)
      val builder = new SubmittedDependencySecretBuilderImpl(
        SECRET_NAME,
        JARS_SECRET,
        FILES_SECRET,
        JARS_SECRET_KEY,
        FILES_SECRET_KEY,
        TRUSTSTORE_SECRET_KEY,
        SSLOptions(trustStore = Some(trustStoreFile)))
      val secret = builder.build()
      val secretDecodedData = decodeSecretData(secret)
      assert(secretDecodedData(TRUSTSTORE_SECRET_KEY) === TRUSTSTORE_STRING_CONTENTS)
    } finally {
      tempTrustStoreDir.delete()
    }
  }

}
