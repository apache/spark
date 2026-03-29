package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;

@Evolving
public interface SupportsOffsetLogUpgrade {
  void migrateMetadataForUpgrade(
      String oldMetadataPath,
      String newMetadataPath,
      long lastBatchId,
      long upgradeBatchId);
}
