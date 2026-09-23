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

/**
 * Thrown when a file cannot support a read the plan depends on, as opposed to being corrupt or
 * missing. The distinction matters because `ignoreCorruptFiles` asks Spark to skip the rest of a
 * file whose read failed, and [[DataSourceUtils.shouldIgnoreCorruptFileException]] excludes this
 * exception for that reason: skipping here would silently drop rows of a perfectly good file.
 *
 * A reader throws this when the plan above it assumes the reader does something the file cannot
 * express. Storage-filter pushdown is the case that motivated it: the planner removes the pushed
 * conjunct from the post-scan `Filter`, so a reader that cannot apply it has to fail the query
 * rather than return rows the filter rejects.
 */
class UnsupportedFileReadException(message: String, cause: Throwable)
  extends RuntimeException(message, cause) {

  def this(message: String) = this(message, null)
}
