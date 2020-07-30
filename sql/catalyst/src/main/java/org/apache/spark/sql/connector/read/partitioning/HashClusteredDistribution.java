package org.apache.spark.sql.connector.read.partitioning;

import org.apache.spark.annotation.Evolving;

import java.util.OptionalInt;

/**
 * A concrete implementation of {@link Distribution}.
 * Represents data where tuples have been clustered according to the hash of the given columns.
 *
 * This is a strictly stronger guarantee than [[ClusteredDistribution]]. Given a tuple and the
 * number of partitions, this distribution strictly requires which partition the tuple should be in.
 *
 * @since 3.x.x
 */
@Evolving
public class HashClusteredDistribution implements Distribution {
    public final String[] clusteredColumns;
    public final OptionalInt numPartitions;

    public HashClusteredDistribution(String[] clusteredColumns, OptionalInt numPartitions) {
        this.clusteredColumns = clusteredColumns;
        this.numPartitions = numPartitions;
    }
}
