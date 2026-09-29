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

import org.apache.spark.sql.connector.catalog.{CatalogPlugin, ChangelogContext, Identifier}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Key for the per-query changelog-state cache in [[AnalysisContext]].
 *
 * A changelog's parsed context and connector-declared state options determine the loaded
 * [[org.apache.spark.sql.connector.catalog.Changelog]]. Other options remain on each relation and
 * are applied when its scan is built.
 */
private[sql] case class ChangelogCacheKey(
    catalog: CatalogPlugin,
    identifier: Identifier,
    context: ChangelogContext,
    stateOptions: CaseInsensitiveStringMap)
