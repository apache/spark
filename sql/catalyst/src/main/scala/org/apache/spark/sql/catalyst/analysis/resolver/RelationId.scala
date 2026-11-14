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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.analysis.TimeTravelSpec
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The [[RelationId]] is a unique identifier for a relation. It is used to lookup the relations
 * which were processed by the [[MetadataResolver]] to substitute the unresolved relations in single
 * pass during the analysis phase. PATH-resolved identifiers retain their original spelling and any
 * frozen path from the current analysis context so distinct resolution scopes cannot share
 * metadata.
 */
case class RelationId(
    multipartIdentifier: Seq[String],
    options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty,
    isStreaming: Boolean = false,
    timeTravelSpec: Option[TimeTravelSpec] = None,
    pathIdentifier: Option[Seq[String]] = None,
    resolutionPathEntries: Option[Seq[Seq[String]]] = None
) {
  def this(
      multipartIdentifier: Seq[String],
      options: CaseInsensitiveStringMap,
      isStreaming: Boolean,
      timeTravelSpec: Option[TimeTravelSpec]) = {
    this(multipartIdentifier, options, isStreaming, timeTravelSpec, None, None)
  }
}

object RelationId {
  def apply(
      multipartIdentifier: Seq[String],
      options: CaseInsensitiveStringMap,
      isStreaming: Boolean,
      timeTravelSpec: Option[TimeTravelSpec]): RelationId = {
    new RelationId(multipartIdentifier, options, isStreaming, timeTravelSpec, None, None)
  }
}
