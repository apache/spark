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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in interface for {@link Scan} that opts the scan in to Spark-side scan merging.
 * <p>
 * By implementing this marker a data source declares a determinism contract: holding the scan
 * options constant, the set of rows and columns the scan reads is fully determined by the filters
 * pushed via {@link SupportsPushDownV2Filters} and the columns pruned via
 * {@link SupportsPushDownRequiredColumns}. Equivalently, rebuilding the scan from a fresh
 * {@link ScanBuilder} with the same options and re-applying the same pushed filters and pruned
 * columns yields an equivalent scan.
 * <p>
 * Given that contract, Spark may fuse two scans of the same table that differ only in their
 * projected columns and/or pushed filters into a single scan. Spark builds the merged scan itself:
 * it obtains a fresh {@link ScanBuilder} from the table, prunes it to the union of both read
 * schemas, re-pushes the (possibly OR-widened) filters, and builds. The merged scan reads the union
 * of the two scans' columns and a superset of their rows; each original scan's result is recovered
 * by a projection and filter applied above it. The connector supplies no merge logic of its own --
 * this interface has no methods.
 * <p>
 * Implementers do not need to reason about pushdowns that are not reproducible this way (a pushed
 * aggregate, join, variant extraction, limit, offset, top-N, or table sample). Spark tracks those
 * on its own side while building the scan and never merges a scan that carries one, whether or not
 * the scan implements this interface. The only obligation of an implementer is the determinism
 * contract above.
 *
 * @since 4.3.0
 */
@Evolving
public interface SupportsScanMerging extends Scan { }
