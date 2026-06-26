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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.join.JoinType;

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
   * <br>
   * <br>
   * For example, JDBC connectors are compatible if they use the same
   * host, port, username, and password.
   */
  boolean isOtherSideCompatibleForJoin(SupportsPushDownJoin other);

  /**
   * Pushes down the join of the current {@code SupportsPushDownJoin} and the other side of join
   * {@code SupportsPushDownJoin}.
   *
   * @param other {@code SupportsPushDownJoin} that this {@code SupportsPushDownJoin}
   * gets joined with.
   * @param joinType the type of join.
   * @param leftSideRequiredColumnsWithAliases required output of the
   *                                           left side {@code SupportsPushDownJoin}
   * @param rightSideRequiredColumnsWithAliases required output of the
   *                                            right side {@code SupportsPushDownJoin}
   * @param condition join condition. Columns are named after the specified aliases in
   * {@code leftSideRequiredColumnsWithAliases} and {@code rightSideRequiredColumnsWithAliases}
   * @return True if join has been successfully pushed down.
   */
  boolean pushDownJoin(
      SupportsPushDownJoin other,
      JoinType joinType,
      ColumnWithAlias[] leftSideRequiredColumnsWithAliases,
      ColumnWithAlias[] rightSideRequiredColumnsWithAliases,
      Predicate condition
  );

  /**
   * Returns pushed operators this data source can preserve when pushing down a join.
   * <p>
   * Spark uses this as a safety check before attempting join push down. If the join input
   * already contains pushed operators not listed here, Spark will not push down the join.
   * This prevents a data source from silently dropping pushed state it does not understand.
   *
   * @since 4.3.0
   */
  default Set<PushedOperator> supportedPushedOperatorsForJoin() {
    return Collections.emptySet();
  }

  /**
   * Pushes down the join of the current {@code SupportsPushDownJoin} and the other side of join
   * {@code SupportsPushDownJoin}, with information about pushed operators from each side.
   *
   * @param other {@code SupportsPushDownJoin} that this {@code SupportsPushDownJoin}
   * gets joined with.
   * @param joinType the type of join.
   * @param leftSideRequiredColumnsWithAliases required output of the
   *                                           left side {@code SupportsPushDownJoin}
   * @param rightSideRequiredColumnsWithAliases required output of the
   *                                            right side {@code SupportsPushDownJoin}
   * @param condition join condition. Columns are named after the specified aliases in
   * {@code leftSideRequiredColumnsWithAliases} and {@code rightSideRequiredColumnsWithAliases}
   * @param joinPushDownInfo information about pushed operators from each side of the join.
   * @return True if join has been successfully pushed down.
   *
   * @since 4.3.0
   */
  default boolean pushDownJoin(
      SupportsPushDownJoin other,
      JoinType joinType,
      ColumnWithAlias[] leftSideRequiredColumnsWithAliases,
      ColumnWithAlias[] rightSideRequiredColumnsWithAliases,
      Predicate condition,
      JoinPushDownInfo joinPushDownInfo) {
    JoinPushDownInfo info =
        joinPushDownInfo == null ? JoinPushDownInfo.empty() : joinPushDownInfo;
    if (info.requiredPushedOperators().isEmpty()) {
      return pushDownJoin(
          other,
          joinType,
          leftSideRequiredColumnsWithAliases,
          rightSideRequiredColumnsWithAliases,
          condition);
    }
    return false;
  }

  /**
   * Pushed operators that may need to be preserved when pushing down a join.
   *
   * @since 4.3.0
   */
  enum PushedOperator {
    TABLE_SAMPLE
  }

  /**
   * Information about pushed operators from each side of the join.
   *
   * @since 4.3.0
   */
  final class JoinPushDownInfo {
    private static final JoinPushDownInfo EMPTY =
        new JoinPushDownInfo(JoinSideInfo.empty(), JoinSideInfo.empty());

    private final JoinSideInfo leftSideInfo;
    private final JoinSideInfo rightSideInfo;

    public JoinPushDownInfo(JoinSideInfo leftSideInfo, JoinSideInfo rightSideInfo) {
      this.leftSideInfo = leftSideInfo == null ? JoinSideInfo.empty() : leftSideInfo;
      this.rightSideInfo = rightSideInfo == null ? JoinSideInfo.empty() : rightSideInfo;
    }

    public static JoinPushDownInfo empty() {
      return EMPTY;
    }

    public JoinSideInfo leftSideInfo() {
      return leftSideInfo;
    }

    public JoinSideInfo rightSideInfo() {
      return rightSideInfo;
    }

    /**
     * Returns result-affecting pushed operators that must be preserved by join push down.
     */
    public Set<PushedOperator> requiredPushedOperators() {
      EnumSet<PushedOperator> operators = EnumSet.noneOf(PushedOperator.class);
      operators.addAll(leftSideInfo.requiredPushedOperators());
      operators.addAll(rightSideInfo.requiredPushedOperators());
      return Collections.unmodifiableSet(operators);
    }
  }

  /**
   * Information about pushed operators from one side of the join.
   *
   * @since 4.3.0
   */
  final class JoinSideInfo {
    private static final JoinSideInfo EMPTY = new JoinSideInfo(null);

    @Nullable
    private final TableSample sample;

    public JoinSideInfo(@Nullable TableSample sample) {
      this.sample = sample;
    }

    public static JoinSideInfo empty() {
      return EMPTY;
    }

    @Nullable
    public TableSample sample() {
      return sample;
    }

    /**
     * Returns result-affecting pushed operators from this side of the join.
     */
    public Set<PushedOperator> requiredPushedOperators() {
      if (sample != null && !sample.isNoOp()) {
        return Collections.unmodifiableSet(EnumSet.of(PushedOperator.TABLE_SAMPLE));
      }
      return Collections.emptySet();
    }
  }

  /**
   * A pushed table sample from one side of the join.
   *
   * @since 4.3.0
   */
  record TableSample(
      double lowerBound,
      double upperBound,
      boolean withReplacement,
      long seed,
      SampleMethod sampleMethod) {
    private boolean isNoOp() {
      return !withReplacement && upperBound - lowerBound >= 1.0;
    }
  }

  /**
   *  A helper class used when there are duplicated names coming from 2 sides of the join
   *  operator.
   *  <br>
   *  Holds information of original output name and the alias of the new output.
   */
  record ColumnWithAlias(String colName, String alias) {
    public String prettyString() {
      if (alias == null) return colName;
      else return colName + " AS " + alias;
    }
  }
}
