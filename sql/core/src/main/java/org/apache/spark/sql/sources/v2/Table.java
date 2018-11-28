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

package org.apache.spark.sql.sources.v2;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.sources.v2.reader.Scan;
import org.apache.spark.sql.sources.v2.reader.ScanBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * An interface representing a logical structured data set of a data source. For example, the
 * implementation can be a directory on the file system, a topic of Kafka, or a table in the
 * catalog, etc.
 * <p>
 * This interface can mixin the following interfaces to support different operations:
 * </p>
 * <ul>
 *   <li>{@link SupportsBatchRead}: this table can be read in batch queries.</li>
 * </ul>
 */
@Evolving
public interface Table {

  /**
   * A name to identify this table.
   * <p>
   * By default this returns the class name of this implementation. Please override it to provide a
   * meaningful name, like the database and table name from catalog, or the location of files for
   * this table.
   * </p>
   */
  default String name() {
    return this.getClass().toString();
  }

  /**
   * Returns the schema of this table.
   */
  StructType schema();

  /**
   * Returns a {@link ScanBuilder} which can be used to build a {@link Scan} later. Spark will call
   * this method for each data scanning query.
   * <p>
   * The builder can take some query specific information to do operators pushdown, and keep these
   * information in the created {@link Scan}.
   * </p>
   */
  ScanBuilder newScanBuilder(DataSourceOptions options);
}
