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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.util.DateTimeUtils

object FileIndexOptions extends DataSourceOptions {
  val IGNORE_MISSING_FILES = newOption(FileSourceOptions.IGNORE_MISSING_FILES)
  val TIME_ZONE = newOption(DateTimeUtils.TIMEZONE_OPTION)
  val RECURSIVE_FILE_LOOKUP = newOption("recursiveFileLookup")
  val BASE_PATH_PARAM = newOption("basePath")
  val MODIFIED_BEFORE = newOption("modifiedbefore")
  val MODIFIED_AFTER = newOption("modifiedafter")
  val PATH_GLOB_FILTER = newOption("pathglobfilter")
}
