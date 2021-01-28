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

package org.apache.spark.sql.connector.distributions;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.SortOrder;

/**
 * Helper methods to create distributions to pass into Spark.
 *
 * @since 3.2.0
 */
@Experimental
public class Distributions {
  private Distributions() {
  }

  /**
   * Creates a distribution where no promises are made about co-location of data.
   */
  public static UnspecifiedDistribution unspecified() {
    return LogicalDistributions.unspecified();
  }

  /**
   * Creates a distribution where tuples that share the same values for clustering expressions are
   * co-located in the same partition.
   */
  public static ClusteredDistribution clustered(Expression[] clustering) {
    return LogicalDistributions.clustered(clustering);
  }

  /**
   * Creates a distribution where tuples have been ordered across partitions according
   * to ordering expressions, but not necessarily within a given partition.
   */
  public static OrderedDistribution ordered(SortOrder[] ordering) {
    return LogicalDistributions.ordered(ordering);
  }
}
