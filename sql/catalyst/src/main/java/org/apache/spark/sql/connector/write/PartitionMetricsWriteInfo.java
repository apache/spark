package org.apache.spark.sql.connector.write;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class PartitionMetricsWriteInfo {

  private final Map<String, PartitionMetrics> metrics = new TreeMap<>();

  public void merge (PartitionMetricsWriteInfo otherAccumulator) {
    otherAccumulator.metrics.forEach((p, m) ->
        metrics.computeIfAbsent(p, key -> new PartitionMetrics(0L, 0L, 0))
            .merge(m));
  }

  public void update(String partitionPath, long bytes, long records, int files) {
    metrics.computeIfAbsent(partitionPath, key -> new PartitionMetrics(0L, 0L, 0))
        .merge(new PartitionMetrics(bytes, records, files));
  }

  public void updateFile(String partitionPath, long bytes, long records) {
    update (partitionPath, bytes, records, 1);
  }

  public Map<String, PartitionMetrics> toMap() {
    return Collections.unmodifiableMap(metrics);
  }

  boolean isZero() {
    return metrics.isEmpty();
  }

  @Override
  public String toString() {
    return "PartitionMetricsWriteInfo{" +
        "metrics=" + metrics +
        '}';
  }
}
