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

package org.apache.spark

import java.io.File
import java.nio.file.{Files => JavaFiles}

import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, render}

object Json4sUtils {

  /** Creates a temp JSON file that contains the input JSON record. */
  def createTempJsonFile(dir: File, prefix: String, jsonValue: JValue): String = {
    val file = File.createTempFile(prefix, ".json", dir)
    JavaFiles.write(file.toPath, compact(render(jsonValue)).getBytes())
    file.getPath
  }
}
