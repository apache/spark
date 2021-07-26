package org.apache.spark.shuffle.checksum;

import java.util.zip.Checksum;

import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.package$;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;

public interface ShuffleChecksumSupport {

  default Checksum[] createPartitionChecksums(int numPartitions, SparkConf conf) {
    if ((boolean) conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ENABLED())) {
      String checksumAlgorithm = conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ALGORITHM());
      return ShuffleChecksumHelper.createPartitionChecksums(numPartitions, checksumAlgorithm);
    } else {
      return ShuffleChecksumHelper.EMPTY_CHECKSUM;
    }
  }

  default long[] getChecksumValues(Checksum[] partitionChecksums) {
    int numPartitions = partitionChecksums.length;
    long[] checksumValues = new long[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      checksumValues[i] = partitionChecksums[i].getValue();
    }
    return checksumValues;
  }
}
