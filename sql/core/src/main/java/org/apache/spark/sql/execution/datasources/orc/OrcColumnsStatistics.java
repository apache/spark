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
 * this class is used to covert ORC {@link ColumnStatistics}s from array to nested tree structure,
 * according to data types. This is used for aggregate push down in ORC.
 */
public class OrcColumnsStatistics {
  private final ColumnStatistics statistics;
  private final List<OrcColumnsStatistics> children;

  public OrcColumnsStatistics(ColumnStatistics statistics) {
    this.statistics = statistics;
    this.children = new ArrayList<>();
  }

  public ColumnStatistics getStatistics() {
    return statistics;
  }

  public OrcColumnsStatistics get(int ordinal) {
    if (ordinal < 0 || ordinal >= children.size()) {
      throw new IndexOutOfBoundsException(
        String.format("Ordinal %d out of bounds of statistics size %d", ordinal, children.size()));
    }
    return children.get(ordinal);
  }

  public void add(OrcColumnsStatistics newChild) {
    children.add(newChild);
  }
}
