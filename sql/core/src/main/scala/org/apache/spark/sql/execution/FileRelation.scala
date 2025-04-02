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

package org.apache.spark.sql.execution

/**
 * An interface for relations that are backed by files.  When a class implements this interface,
 * the list of paths that it returns will be returned to a user who calls `inputPaths` on any
 * DataFrame that queries this relation.
 */
trait FileRelation {
  /**
   * Returns the list of files that will be read when scanning this relation.
   * The strings returned are expected to be url-encoded paths.
   */
  def inputFiles: Array[String]
}
