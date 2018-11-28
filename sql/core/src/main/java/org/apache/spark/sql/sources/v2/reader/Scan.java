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

package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.sources.v2.SupportsBatchRead;
import org.apache.spark.sql.sources.v2.Table;

/**
 * A logical representation of a data source scan. This interface is used to provide logical
 * information, like what the actual read schema is.
 * <p>
 * This logical representation is shared between batch scan, micro-batch streaming scan and
 * continuous streaming scan. Data sources must implement the corresponding methods in this
 * interface, to match what the table promises to support. For example, {@link #toBatch()} must be
 * implemented, if the {@link Table} that creates this {@link Scan} implements
 * {@link SupportsBatchRead}.
 * </p>
 */
@Evolving
public interface Scan {

  /**
   * Returns the actual schema of this data source scan, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   */
  StructType readSchema();

  /**
   * Returns the physical representation of this scan for batch query. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this scan implements {@link SupportsBatchRead}.
   */
  default Batch toBatch() {
    throw new UnsupportedOperationException("Batch scans are not supported");
  }
}
