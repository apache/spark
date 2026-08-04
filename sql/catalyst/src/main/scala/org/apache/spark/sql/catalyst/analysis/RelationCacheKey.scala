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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Key for the per-query relation cache in [[AnalysisContext]], shared by [[RelationResolution]].
 *
 * Options are part of the key because a catalog's options-aware `loadTable` can return a different
 * `Table` depending on them, so two references to the same identifier with different options must
 * not share a cached relation.
 */
private[sql] case class RelationCacheKey(
    nameParts: Seq[String],
    timeTravelSpec: Option[TimeTravelSpec],
    options: CaseInsensitiveStringMap)
