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

package org.apache.spark.sql.execution.joins;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.util.collection.BitSet;
import org.apache.spark.util.collection.OpenHashSet;

/**
 * Static helpers shared by join operators in this package, used both from whole-stage codegen and
 * from interpreted execution paths. Hoisting recurring snippets here (especially the ones that
 * would otherwise be emitted as anonymous inner classes per generated stage) keeps the generated
 * Java source smaller and lets the JIT compile the bodies once instead of once per stage.
 */
public final class JoinHelper {

  private JoinHelper() {}

  /**
   * Reset a Spark {@link org.apache.spark.util.collection.BitSet} (not {@link java.util.BitSet})
   * that tracks which rows in a buffer of size {@code bufferSize} have already been matched.
   * Reuses {@code matched} when its capacity is sufficient; otherwise returns a freshly allocated
   * BitSet. Callers must assign the returned reference back to their bit-set field.
   *
   * <p>Used by full-outer sort-merge join, where the left- and right-side buffers are repopulated
   * for each batch of rows sharing a join key.
   */
  public static BitSet resetMatched(BitSet matched, int bufferSize) {
    if (bufferSize <= matched.capacity()) {
      matched.clearUntil(bufferSize);
      return matched;
    }
    return new BitSet(bufferSize);
  }

  /**
   * Register a task-completion listener that adds the final spill size of {@code matches} to
   * {@code spillSize}. Replaces an anonymous {@code TaskCompletionListener} that would otherwise
   * be generated per {@code SortMergeJoinExec} whole-stage class.
   */
  public static void recordSpillSizeOnTaskCompletion(
      ExternalAppendOnlyUnsafeRowArray matches, SQLMetric spillSize) {
    TaskContext.get().addTaskCompletionListener(context -> {
      spillSize.add(matches.spillSize());
    });
  }

  /**
   * Register a task-completion listener that adds the estimated memory footprint of
   * {@code matchedRows} (the bit-set plus the data array) to {@code metric}. Used by
   * {@code ShuffledHashJoinExec} to track {@code buildDataSize} for its matched-row tracker.
   */
  public static void recordOpenHashSetMemoryUsageOnTaskCompletion(
      OpenHashSet<?> matchedRows, SQLMetric metric) {
    TaskContext.get().addTaskCompletionListener(context -> {
      long bitSetEstimatedSize = matchedRows.getBitSet().capacity() / 8L;
      long dataEstimatedSize = matchedRows.capacity() * 8L;
      metric.add(bitSetEstimatedSize + dataEstimatedSize);
    });
  }
}
