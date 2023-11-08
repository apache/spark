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

import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{Schema, SchemaFactory}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFiles
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.Utils

/**
 * Utilities for working with XSD validation.
 */
private[sql] object ValidatorUtil {
  // Parsing XSDs may be slow, so cache them by path:

  private val cache = CacheBuilder.newBuilder().softValues().build(
    new CacheLoader[String, Schema] {
      override def load(key: String): Schema = {
        val in = try {
          // Handle case where file exists as specified
          val fs = Utils.getHadoopFileSystem(key, SparkHadoopUtil.get.conf)
          fs.open(new Path(key))
        } catch {
          case _: Throwable =>
            // Handle case where it was added with sc.addFile
            val addFileUrl = SparkFiles.get(key)
            val fs = Utils.getHadoopFileSystem(addFileUrl, SparkHadoopUtil.get.conf)
            fs.open(new Path(addFileUrl))
        }
        try {
          val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
          schemaFactory.newSchema(new StreamSource(in))
        } finally {
          in.close()
        }
      }
    })

  /**
   * Parses the XSD at the given local path and caches it.
   *
   * @param path path to XSD
   * @return Schema for the file at that path
   */
  def getSchema(path: String): Schema = cache.get(path)
}
