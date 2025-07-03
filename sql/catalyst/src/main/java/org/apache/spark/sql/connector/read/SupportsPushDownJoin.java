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

import java.util.Optional;

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
   * current {@code SupportsPushDownJoin} for a join push down, meaning both sides can be
   * processed together within the same underlying data source.
   *
   * <p>For example, JDBC connectors are compatible if they use the same
   * host, port, username, and password.</p>
   */
  boolean isOtherSideCompatibleForJoin(SupportsPushDownJoin other);

  /**
   * Joining 2 {@code SupportsPushDownJoin} can be problematic if there are columns with duplicate
   * names. The {@code SupportsPushDownJoin} implementation should deal with this problem.
   *
   * This method returns the merged schema that will be preserved to {@code SupportsPushDownJoin}'s
   * schema after {@code pushDownJoin} call succeeds.
   *
   * @param other {@code SupportsPushDownJoin} that this {@code SupportsPushDownJoin}
   * gets joined with.
   * @param requiredSchema required columns needed for this {@code SupportsPushDownJoin}.
   * @param otherSideRequiredSchema required columns needed for other {@code SupportsPushDownJoin}.
   * @return merged schema. If ambiguous names are forbidden, the merged schema should resolve that.
   */
  StructType getJoinedSchema(
    SupportsPushDownJoin other,
    StructType requiredSchema,
    StructType otherSideRequiredSchema
  );

  /**
   * Pushes down the join of the current {@code SupportsPushDownJoin} and the other side of join
   * {@code SupportsPushDownJoin}.
   *
   * @param other {@code SupportsPushDownJoin} that this {@code SupportsPushDownJoin}
   * gets joined with.
   * @param requiredSchema required output schema after join push down.
   * @param joinType the type of join.
   * @param condition join condition.
   * @return True if join has been successfully pushed down.
   */
  boolean pushDownJoin(
    SupportsPushDownJoin other,
    StructType requiredSchema,
    JoinType joinType,
    Predicate condition
  );
}
