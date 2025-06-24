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

package org.apache.spark.sql.pipelines.util

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.pipelines.util.StreamingReadOptions.EmptyUserOptions

/**
 * Generic options for a read of an input.
 */
sealed trait InputReadOptions

/**
 * Options for a batch read of an input.
 */
final case class BatchReadOptions() extends InputReadOptions

/**
 * Options for a streaming read of an input.
 *
 * @param userOptions Holds the user defined read options.
 * @param droppedUserOptions Holds the options that were specified by the user but
 *                       not actually used. This is a bug but we are preserving this behavior
 *                       for now to avoid making a backwards incompatible change.
 */
final case class StreamingReadOptions(
    userOptions: CaseInsensitiveMap[String] = EmptyUserOptions,
    droppedUserOptions: CaseInsensitiveMap[String] = EmptyUserOptions
) extends InputReadOptions

object StreamingReadOptions {
  val EmptyUserOptions: CaseInsensitiveMap[String] = CaseInsensitiveMap(Map())
}
