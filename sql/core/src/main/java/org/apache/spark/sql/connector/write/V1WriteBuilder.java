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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Unstable;
import org.apache.spark.sql.sources.InsertableRelation;

/**
 * A trait that should be implemented by V1 DataSources that would like to leverage the DataSource
 * V2 write code paths. The InsertableRelation will be used only to Append data. Other
 * instances of the [[WriteBuilder]] interface such as [[SupportsOverwrite]], [[SupportsTruncate]]
 * should be extended as well to support additional operations other than data appends.
 *
 * This interface is designed to provide Spark DataSources time to migrate to DataSource V2 and
 * will be removed in a future Spark release.
 *
 * @since 3.0.0
 */
@Unstable
public interface V1WriteBuilder extends WriteBuilder {
  /**
   * Creates an InsertableRelation that allows appending a DataFrame to a
   * a destination (using data source-specific parameters). The insert method will only be
   * called with `overwrite=false`. The DataSource should implement the overwrite behavior as
   * part of the [[SupportsOverwrite]], and [[SupportsTruncate]] interfaces.
   *
   * @since 3.0.0
   */
  InsertableRelation buildForV1Write();
}
