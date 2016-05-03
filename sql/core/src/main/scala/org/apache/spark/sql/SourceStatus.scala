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

package org.apache.spark.sql

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.streaming.{Offset, Source}

/**
 * :: Experimental ::
 * Status and metrics of a streaming [[Source]].
 *
 * @param description     Description of the source corresponding to this status
 * @param offset          Current offset of the source, if known
 * @since 2.0.0
 */
@Experimental
class SourceStatus private[sql] (
    val description: String,
    val offset: Option[Offset])
