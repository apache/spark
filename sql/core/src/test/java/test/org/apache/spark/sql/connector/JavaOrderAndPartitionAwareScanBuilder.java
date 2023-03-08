package test.org.apache.spark.sql.connector;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.SupportsReportOrdering;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;

public class JavaOrderAndPartitionAwareScanBuilder extends JavaPartitionAwareScanBuilder
    implements SupportsReportOrdering {

    private final Partitioning partitioning;
    private final SortOrder[] ordering;

    JavaOrderAndPartitionAwareScanBuilder(InputPartition[] partitions, String partitionKeys, String orderKeys) {
        super(partitions);

        if (partitionKeys != null) {
            String[] keys = partitionKeys.split(",");
            Expression[] clustering = new Transform[keys.length];
            for (int i = 0; i < keys.length; i++) {
                clustering[i] = Expressions.identity(keys[i]);
            }
            this.partitioning = new KeyGroupedPartitioning(clustering, 2);
        } else {
            this.partitioning = new UnknownPartitioning(2);
        }

        if (orderKeys != null) {
            String[] keys = orderKeys.split(",");
            this.ordering = new SortOrder[keys.length];
            for (int i = 0; i < keys.length; i++) {
                this.ordering[i] = new JavaOrderAndPartitionAwareDataSource.MySortOrder(keys[i]);
            }
        } else {
            this.ordering = new SortOrder[0];
        }
    }

    @Override
    public Partitioning outputPartitioning() {
        return this.partitioning;
    }

    @Override
    public SortOrder[] outputOrdering() {
        return this.ordering;
    }

}
