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

package org.apache.spark.sql.catalog.v2;

/**
 * A logical transformation function.
 * <p>
 * This does not support applying transformations; it only communicates the type of transformation
 * and its input column references.
 * <p>
 * This interface is used to pass partitioning transformations to v2 catalog implementations. For
 * example a table may partition data by the date of a timestamp column, ts, using
 * <code>date(ts)</code>. This is similar to org.apache.spark.sql.sources.Filter, which is used to
 * pass boolean filter expressions to data source implementations.
 * <p>
 * To use data values directly as partition values, use the "identity" transform:
 * <code>identity(col)</code>. Identity partition transforms are the only transforms used by Hive.
 * For Hive tables, SQL statements produce data columns that are used without modification to
 * partition the remaining data columns.
 * <p>
 * Table formats other than Hive can use partition transforms to automatically derive partition
 * values from rows and to transform data predicates to partition predicates.
 */
public interface PartitionTransform {
  /**
   * The name of this transform.
   */
  String name();

  /**
   * The data columns that are referenced by this transform.
   */
  String[] references();
}
