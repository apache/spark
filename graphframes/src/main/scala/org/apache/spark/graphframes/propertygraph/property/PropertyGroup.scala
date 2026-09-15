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

package org.apache.spark.graphframes.propertygraph.property

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

trait PropertyGroup {
  val name: String
  val data: DataFrame
  protected def validate(): this.type

  /**
   * Returns a view of the data for the property group without applying any filter.
   *
   * @return
   *   A DataFrame containing the raw data.
   */
  protected[graphframes] def getData(): DataFrame = getData(lit(true))

  /**
   * Returns a filtered view of the data for the property group, with an optional mask applied to
   * IDs.
   *
   * @param filter
   *   A condition (Column) used to filter the data.
   * @return
   *   A DataFrame containing the filtered and optionally transformed data.
   */
  protected[graphframes] def getData(filter: Column): DataFrame
}
