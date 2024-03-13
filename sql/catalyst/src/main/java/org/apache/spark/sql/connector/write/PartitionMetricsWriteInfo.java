package org.apache.spark.sql.connector.write;

import java.util.Map;
import java.util.TreeMap;

public class PartitionMetricsWriteInfo {

  private final Map<String, PartitionMetrics> metrics = new TreeMap<>();

  public void merge (PartitionMetricsWriteInfo otherAccumulator) {
    otherAccumulator.metrics.forEach((p, m) ->
        metrics.computeIfAbsent(p, key -> new PartitionMetrics())
            .merge(m));
  }

  public void updateFile(String partitionPath, long bytes, long records) {
    metrics.computeIfAbsent(partitionPath, key -> new PartitionMetrics())
        .updateFile(bytes, records);
  }

  @Override
  public String toString() {
    return "PartitionMetricsWriteInfo{" +
        "metrics=" + metrics +
        '}';
  }
}
