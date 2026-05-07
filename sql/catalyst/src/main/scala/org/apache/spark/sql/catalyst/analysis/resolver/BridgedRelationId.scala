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

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

/**
 * The [[BridgedRelationId]] is a unique identifier for an unresolved relation in the whole logical
 * plan including all the nested views. It is used to lookup relations with resolved metadata which
 * were processed by the fixed-point when running two Analyzers in dual-run mode. Storing
 * [[catalogAndNamespace]] is required to differentiate tables/views created in different catalogs
 * as their [[UnresolvedRelation]]s could have same structure.
 */
case class BridgedRelationId(
    unresolvedRelation: UnresolvedRelation,
    catalogAndNamespace: Seq[String]
)
