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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.Statistics;

/**
 * A mix-in interface for {@link Table}. Data sources can implement this interface to report
 * table-level statistics to Spark.
 * <p>
 * The statistics returned here describe the table as a whole, independent of any {@code Scan}
 * and independent of any pushed operators (filters, partition pruning, column pruning,
 * aggregate pushdown). They are analogous to DSv1's {@code CatalogStatistics} exposed via
 * {@code CatalogTable.stats}: a stable, pre-filter, pre-pruning property of the table.
 * <p>
 * This is distinct from
 * {@link org.apache.spark.sql.connector.read.SupportsReportStatistics} on {@code Scan}, which
 * reports <em>post-pushdown</em> statistics that reflect the data the scan will actually
 * produce. A table may implement both: catalog statistics drive CBO decisions on the
 * unfiltered relation (join reordering, broadcast thresholds), while scan statistics tighten
 * estimates once pushdown has happened.
 * <p>
 * Implementations should return statistics without triggering expensive work such as file
 * listing or remote metadata fetches beyond what is already cached on the {@code Table}
 * instance. Consumers may call this method during optimization, potentially more than once
 * per query, and expect a consistent result for a given {@code Table} instance.
 *
 * @since 4.2.0
 */
@Evolving
public interface SupportsReportCatalogStatistics extends Table {

  /**
   * Returns table-level statistics for this table.
   * <p>
   * The returned statistics must describe the full table, not a filtered or pruned subset.
   * When no statistics are available, return {@link Statistics#emptyStatistics} rather than
   * {@code null}.
   */
  Statistics catalogStatistics();
}
