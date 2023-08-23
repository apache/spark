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

import java.nio.file.Paths
import javax.xml.XMLConstants
import javax.xml.validation.{Schema, SchemaFactory}

import com.google.common.cache.{CacheBuilder, CacheLoader}

import org.apache.spark.SparkFiles

/**
 * Utilities for working with XSD validation.
 */
private[sql] object ValidatorUtil {

  // Parsing XSDs may be slow, so cache them by path:

  private val cache = CacheBuilder.newBuilder().softValues().build(
    new CacheLoader[String, Schema] {
      override def load(key: String): Schema = {
        // Handle case where file exists as specified
        var path = Paths.get(key)
        if (!path.toFile.exists()) {
          // Handle case where it was added with sc.addFile
          path = Paths.get(SparkFiles.get(key))
        }
        val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
        schemaFactory.newSchema(path.toFile)
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
