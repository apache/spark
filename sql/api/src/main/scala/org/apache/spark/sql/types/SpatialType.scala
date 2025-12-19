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

package org.apache.spark.sql.types

import org.apache.spark.annotation.Unstable

@Unstable
trait SpatialType extends AbstractDataType {

  /**
   * Mixed SRID value and the corresponding CRS for geospatial types (Geometry and Geography).
   * These values represent a geospatial type that can hold different SRID values per row.
   */
  final val MIXED_SRID: Int = -1
  final val MIXED_CRS: String = "SRID:ANY"
}
