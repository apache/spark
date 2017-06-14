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
package org.apache.spark.deploy.rest.kubernetes

import java.io.{File, FileInputStream, FileOutputStream, InputStreamReader}
import java.security.{KeyStore, PrivateKey}
import java.security.cert.Certificate
import java.util.UUID

import com.google.common.base.Charsets
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.openssl.{PEMKeyPair, PEMParser}
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter
import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.util.Utils

private[spark] object PemsToKeyStoreConverter {

  /**
   * Loads the given key-cert pair into a temporary keystore file. Returns the File pointing
   * to where the keyStore was written to disk.
   */
  def convertPemsToTempKeyStoreFile(
      keyPemFile: File,
      certPemFile: File,
      keyAlias: String,
      keyStorePassword: String,
      keyPassword: String,
      keyStoreType: Option[String]): File = {
    require(keyPemFile.isFile, s"Key PEM file provided at ${keyPemFile.getAbsolutePath}" +
      " does not exist or is not a file.")
    require(certPemFile.isFile, s"Cert PEM file provided at ${certPemFile.getAbsolutePath}" +
      " does not exist or is not a file.")
    val privateKey = parsePrivateKeyFromPemFile(keyPemFile)
    val certificates = parseCertificatesFromPemFile(certPemFile)
    val resolvedKeyStoreType = keyStoreType.getOrElse(KeyStore.getDefaultType)
    val keyStore = KeyStore.getInstance(resolvedKeyStoreType)
    keyStore.load(null, null)
    keyStore.setKeyEntry(
      keyAlias,
      privateKey,
      keyPassword.toCharArray,
      certificates)
    val keyStoreDir = Utils.createTempDir("temp-keystores")
    val keyStoreFile = new File(keyStoreDir, s"keystore-${UUID.randomUUID()}.$resolvedKeyStoreType")
    Utils.tryWithResource(new FileOutputStream(keyStoreFile)) { storeStream =>
      keyStore.store(storeStream, keyStorePassword.toCharArray)
    }
    keyStoreFile
  }

  def convertCertPemToTrustStore(
      certPemFile: File,
      trustStoreType: Option[String]): KeyStore = {
    require(certPemFile.isFile, s"Cert PEM file provided at ${certPemFile.getAbsolutePath}" +
      " does not exist or is not a file.")
    val trustStore = KeyStore.getInstance(trustStoreType.getOrElse(KeyStore.getDefaultType))
    trustStore.load(null, null)
    parseCertificatesFromPemFile(certPemFile).zipWithIndex.foreach { case (cert, index) =>
      trustStore.setCertificateEntry(s"certificate-$index", cert)
    }
    trustStore
  }

  def convertCertPemToTempTrustStoreFile(
      certPemFile: File,
      trustStorePassword: String,
      trustStoreType: Option[String]): File = {
    val trustStore = convertCertPemToTrustStore(certPemFile, trustStoreType)
    val tempTrustStoreDir = Utils.createTempDir(namePrefix = "temp-trustStore")
    val tempTrustStoreFile = new File(tempTrustStoreDir,
      s"trustStore.${trustStoreType.getOrElse(KeyStore.getDefaultType)}")
    Utils.tryWithResource(new FileOutputStream(tempTrustStoreFile)) {
      trustStore.store(_, trustStorePassword.toCharArray)
    }
    tempTrustStoreFile
  }

  private def withPemParsedFromFile[T](pemFile: File)(f: (PEMParser => T)): T = {
    Utils.tryWithResource(new FileInputStream(pemFile)) { pemStream =>
      Utils.tryWithResource(new InputStreamReader(pemStream, Charsets.UTF_8)) { pemReader =>
        Utils.tryWithResource(new PEMParser(pemReader))(f)
      }
    }
  }

  private def parsePrivateKeyFromPemFile(keyPemFile: File): PrivateKey = {
    withPemParsedFromFile(keyPemFile) { keyPemParser =>
      val converter = new JcaPEMKeyConverter
      keyPemParser.readObject() match {
        case privateKey: PrivateKeyInfo =>
          converter.getPrivateKey(privateKey)
        case keyPair: PEMKeyPair =>
          converter.getPrivateKey(keyPair.getPrivateKeyInfo)
        case _ =>
          throw new SparkException(s"Key file provided at ${keyPemFile.getAbsolutePath}" +
            s" is not a key pair or private key PEM file.")
      }
    }
  }

  private def parseCertificatesFromPemFile(certPemFile: File): Array[Certificate] = {
    withPemParsedFromFile(certPemFile) { certPemParser =>
      val certificates = mutable.Buffer[Certificate]()
      var pemObject = certPemParser.readObject()
      while (pemObject != null) {
        pemObject match {
          case certificate: X509CertificateHolder =>
            val converter = new JcaX509CertificateConverter
            certificates += converter.getCertificate(certificate)
          case _ =>
        }
        pemObject = certPemParser.readObject()
      }
      if (certificates.isEmpty) {
        throw new SparkException(s"No certificates found in ${certPemFile.getAbsolutePath}")
      }
      certificates.toArray
    }
  }
}
