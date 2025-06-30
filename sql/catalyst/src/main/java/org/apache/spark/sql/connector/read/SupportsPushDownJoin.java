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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.join.JoinType;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * push down join operators.
 *
 * @since 4.1.0
 */
@Evolving
public interface SupportsPushDownJoin extends ScanBuilder {
  /**
   * Returns true if the other side of the join is compatible with the
   * current {@code SupportsPushDownJoin} for a join push-down, meaning both sides can be
   * processed together within the same underlying data source.
   *
   * <p>For example, JDBC connectors are compatible if they use the same
   * host, port, username, and password.</p>
   */
  boolean isOtherSideCompatibleForJoin(SupportsPushDownJoin other);

  /**
   * Pushes down the join of the current {@code SupportsPushDownJoin} and the other side of join
   * {@code SupportsPushDownJoin}.
   *
   * {@code leftSideQualifier} and {@code rightSideQualifier} are the qualifiers of left and right
   * side of the join. These are used to fully qualify column names in case of the duplicated ones.
   *
   * {@code leftRequiredSchema} and {@code rightRequiredSchema} are the projected schemas
   * from both left and right side of the join.
   *
   * @return True if join has been successfully pushed down.
   */
  boolean pushJoin(
    SupportsPushDownJoin other,
    JoinType joinType,
    Predicate condition,
    String[] leftSideQualifier,
    String[] rightSideQualifier,
    StructType leftRequiredSchema,
    StructType rightRequiredSchema
  );
}
