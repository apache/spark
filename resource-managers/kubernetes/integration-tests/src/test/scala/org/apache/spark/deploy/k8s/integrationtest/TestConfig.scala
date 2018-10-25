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
package org.apache.spark.deploy.k8s.integrationtest

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files

object TestConfig {
  def getTestImageTag: String = {
    val imageTagFileProp = System.getProperty("spark.kubernetes.test.imageTagFile")
    require(imageTagFileProp != null, "Image tag file must be provided in system properties.")
    val imageTagFile = new File(imageTagFileProp)
    require(imageTagFile.isFile, s"No file found for image tag at ${imageTagFile.getAbsolutePath}.")
    Files.toString(imageTagFile, Charsets.UTF_8).trim
  }

  def getTestImageRepo: String = {
    val imageRepo = System.getProperty("spark.kubernetes.test.imageRepo")
    require(imageRepo != null, "Image repo must be provided in system properties.")
    imageRepo
  }
}
