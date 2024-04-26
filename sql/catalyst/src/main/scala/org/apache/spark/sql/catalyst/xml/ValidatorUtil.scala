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
package org.apache.spark.sql.catalyst.xml

import java.io.{File, FileInputStream, InputStream}
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{Schema, SchemaFactory}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFiles
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.MDC

/**
 * Utilities for working with XSD validation.
 */
object ValidatorUtil extends Logging {
  // Parsing XSDs may be slow, so cache them by path:

  private val cache = CacheBuilder.newBuilder().softValues().build(
    new CacheLoader[String, Schema] {
      override def load(key: String): Schema = {
        val in = openSchemaFile(new Path(key))
        try {
          val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
          schemaFactory.newSchema(new StreamSource(in, key))
        } finally {
          in.close()
        }
      }
    })

  def openSchemaFile(xsdPath: Path): InputStream = {
    try {
      // Handle case where file exists as specified
      val fs = xsdPath.getFileSystem(SparkHadoopUtil.get.conf)
      fs.open(xsdPath)
    } catch {
      case e: Throwable =>
        // Handle case where it was added with sc.addFile
        // When they are added via sc.addFile, they are always downloaded to local file system
        logInfo(log"${MDC(XSD_PATH, xsdPath)} was not found, " +
          log"falling back to look up files added by Spark")
        val f = new File(SparkFiles.get(xsdPath.toString))
        if (f.exists()) {
          new FileInputStream(f)
        } else {
          throw e
        }
    }
  }

  /**
   * Parses the XSD at the given local path and caches it.
   *
   * @param path path to XSD
   * @return Schema for the file at that path
   */
  def getSchema(path: String): Schema = cache.get(path)
}
