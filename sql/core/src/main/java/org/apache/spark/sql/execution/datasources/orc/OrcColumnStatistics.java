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

package org.apache.spark.sql.execution.datasources.orc;

import org.apache.orc.ColumnStatistics;

import java.util.ArrayList;
import java.util.List;

/**
 * Columns statistics interface wrapping ORC {@link ColumnStatistics}s.
 *
 * Because ORC {@link ColumnStatistics}s are stored as an flatten array in ORC file footer,
 * this class is used to convert ORC {@link ColumnStatistics}s from array to nested tree structure,
 * according to data types. The flatten array stores all data types (including nested types) in
 * tree pre-ordering. This is used for aggregate push down in ORC.
 *
 * For nested data types (array, map and struct), the sub-field statistics are stored recursively
 * inside parent column's children field. Here is an example of {@link OrcColumnStatistics}:
 *
 * Data schema:
 * c1: int
 * c2: struct<f1: int, f2: float>
 * c3: map<key: int, value: string>
 * c4: array<int>
 *
 *                        OrcColumnStatistics
 *                                | (children)
 *             ---------------------------------------------
 *            /         |                 \                 \
 *           c1        c2                 c3                c4
 *      (integer)    (struct)            (map)             (array)
*        (min:1,        | (children)       | (children)      | (children)
 *       max:10)      -----              -----             element
 *                   /     \            /     \           (integer)
 *                c2.f1    c2.f2      key     value
 *              (integer) (float)  (integer) (string)
 *                       (min:0.1,           (min:"a",
 *                       max:100.5)          max:"zzz")
 */
public class OrcColumnStatistics {
  private final ColumnStatistics statistics;
  private final List<OrcColumnStatistics> children;

  public OrcColumnStatistics(ColumnStatistics statistics) {
    this.statistics = statistics;
    this.children = new ArrayList<>();
  }

  public ColumnStatistics getStatistics() {
    return statistics;
  }

  public OrcColumnStatistics get(int ordinal) {
    if (ordinal < 0 || ordinal >= children.size()) {
      throw new IndexOutOfBoundsException(
        String.format("Ordinal %d out of bounds of statistics size %d", ordinal, children.size()));
    }
    return children.get(ordinal);
  }

  public void add(OrcColumnStatistics newChild) {
    children.add(newChild);
  }
}
