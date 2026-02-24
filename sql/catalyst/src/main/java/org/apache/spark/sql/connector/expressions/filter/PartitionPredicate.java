package org.apache.spark.sql.connector.expressions.filter;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Expression;

/**
 * Represents a partition filter expression (an expression targeting only the schema of
 * {@link org.apache.spark.sql.connector.catalog.Table#partitioning()}).
 * <p>
 * This can be used to evaluate individual partition keys against this partition expression
 * by {@link #accept(InternalRow)}.
 * </p>
 * @since 4.2.0
 */
public abstract class PartitionPredicate extends Predicate {

  /**
   * Default predicate name for partition predicates.
   */
  public static final String NAME = "PartitionPredicate";

  public PartitionPredicate(String name, Expression[] children) {
    super(name, children);
  }

  /**
   * Evaluates this predicate against a single partition's keys.
   *
   * @param partitionKey keys of a single partition, represented the values of the partition
   *                     corresponding to
   *                     {@link org.apache.spark.sql.connector.catalog.Table#partitioning()}
   * @return true if this partition evaluates to true for this partition expression.
   */
  public abstract boolean accept(InternalRow partitionKey);

  /**
   * Returns the ordinal position(s) of the partition transform(s) in
   * {@link org.apache.spark.sql.connector.catalog.Table#partitioning()} that are
   * referenced by this partition filter expression.
   *
   * <p><b>Example:</b> Suppose {@code Table.partitioning()} returns three partition
   * transforms: {@code [years(ts), months(ts), bucket(32, id)]} with ordinals 0, 1, 2.
   * <ul>
   *   <li>A filter expression {@code years(ts) = 2026} returns {@code [0]}.</li>
   *   <li>A filter expression {@code years(ts) = 2026 and months(ts) = 01}
   *       returns {@code [0, 1]}.</li>
   *   <li>A filter expression {@code bucket(32, id) = 1} returns {@code [2]}.</li>
   * </ul>
   * <p>
   * Data sources can use this to evaluate PartitionPredicates pushed down by
   * {@link org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering#filter(Predicate[])}
   * to determine whether the PartitionPredicate can be satisfied completely,
   * or whether it must be returned to Spark for post-scan filtering.
   * <p>
   * For example, data sources supporting partition spec evolution
   * should return PartitionPredicates that reference later-added partition
   * transforms (for which data in the the table is incompletely partitioned)
   * to Spark for post-scan filter. Initially-added partition transforms
   * (for which data in the table is completely partitioned) do not need to be returned
   * for post-scan filter.
   * @return array of 0-based ordinals for the transform(s) in
   * {@link org.apache.spark.sql.connector.catalog.Table#partitioning()} referenced by this
   * PartitionPredicate's partition filter expression.
   */
  public abstract int[] referencedPartitionColumnOrdinals();
}
