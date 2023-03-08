package test.org.apache.spark.sql.connector;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;

public class JavaPartitionAwareScanBuilder
    extends JavaSimpleScanBuilder implements SupportsReportPartitioning {
    private final InputPartition[] partitions;

    public JavaPartitionAwareScanBuilder(InputPartition[] partitions) {
        this.partitions = partitions;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return this.partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new JavaPartitionAwareDataSource.SpecificReaderFactory();
    }

    @Override
    public Partitioning outputPartitioning() {
        Expression[] clustering = new Transform[] { Expressions.identity("i") };
        return new KeyGroupedPartitioning(clustering, partitions.length);
    }
}

