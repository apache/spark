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

package org.apache.spark.sql.connector.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.annotation.Evolving;

/**
 * A custom file task metric. This allows file based data source V2 implementations
 * to use a single PartitionReader with multiple file readers. Each file reader can
 * provide its own metrics values and they can be added into the parent PartitionReader
 * @since 4.0.0
 */
@Evolving
public interface CustomFileTaskMetric extends CustomTaskMetric {

  /**
   * Updates the underlying value of the metric by adding addValue to the existing value
   */
  default void update(long addValue) {}


  /*
  Merge(add) the values of the corresponding CustomTaskMetric from src array into target array
  adding a new element if it doesn't already exist. Returns a new array without modifying the
  target array
  */
  static List<CustomFileTaskMetric> mergeMetricValues(List<CustomTaskMetric> src,
      List<CustomFileTaskMetric> target) {
    List<CustomFileTaskMetric> out = new ArrayList<>(target);
    src.forEach(srcMetric -> {
          Optional<CustomFileTaskMetric> m = out.stream()
              .filter(targetMetric -> targetMetric.name().equals(srcMetric.name()))
              .findFirst();
          if (m.isPresent()) {
            m.get().update(srcMetric.value());
          } else {
            if (srcMetric instanceof CustomFileTaskMetric) {
              out.add((CustomFileTaskMetric) srcMetric);
            }
          }
        }
    );
    return out;
  }
}
