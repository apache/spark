package org.apache.spark.sql.connector.write;

import org.apache.spark.sql.connector.read.partitioning.Distribution;

public interface BatchWriteRequiresDistribution extends WriteBuilder {
    /**
     * Returns the output data partitioning that this reader guarantees.
     */
    Distribution requiredDistribution();
}
