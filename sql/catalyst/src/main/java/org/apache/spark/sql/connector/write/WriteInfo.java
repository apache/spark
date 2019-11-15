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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.types.StructType;

/**
 * This interface contains write information that data sources can use when generating a
 * {@link WriteBuilder}.
 */
@Experimental
public interface WriteInfo {
  /**
   * @return `queryId` is a unique string of the query. It's possible that there are many queries
   * running at the same time, or a query is restarted and resumed. {@link BatchWrite} can use
   * this id to identify the query.
   */
  String queryId();

  /**
   * @return the schema of the input data from Spark to data source.
   */
  StructType schema();
}
