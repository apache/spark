package org.apache.spark.sql.connector.catalog;

import org.apache.spark.sql.connector.read.partitioning.Distribution;

public interface RequiresTableWriteDistribution extends SupportsWrite {
    /**
     * Returns the output data partitioning that this reader guarantees.
     */
    Distribution requiredDistribution();
}
