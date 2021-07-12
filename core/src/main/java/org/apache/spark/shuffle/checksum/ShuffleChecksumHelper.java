package org.apache.spark.shuffle.checksum;

import java.util.Locale;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.internal.config.package$;
import org.apache.spark.storage.ShuffleChecksumBlockId;

public class ShuffleChecksumHelper {

  public static boolean isShuffleChecksumEnabled(SparkConf conf) {
    return (boolean) conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ENABLED());
  }

  public static Checksum[] createPartitionChecksumsIfEnabled(int numPartitions, SparkConf conf)
    throws SparkException {
    Checksum[] partitionChecksums;

    if (!isShuffleChecksumEnabled(conf)) {
      partitionChecksums = new Checksum[0];
      return partitionChecksums;
    }

    String checksumAlgo = shuffleChecksumAlgorithm(conf);
    switch (checksumAlgo) {
      case "ADLER32":
        partitionChecksums = new Adler32[numPartitions];
        for (int i = 0; i < numPartitions; i ++) {
          partitionChecksums[i] = new Adler32();
        }
        return partitionChecksums;

      case "CRC32":
        partitionChecksums = new CRC32[numPartitions];
        for (int i = 0; i < numPartitions; i ++) {
          partitionChecksums[i] = new CRC32();
        }
        return partitionChecksums;

      default:
        throw new SparkException("Unsupported shuffle checksum algorithm: " + checksumAlgo);
    }
  }

  public static long[] getChecksumValues(Checksum[] partitionChecksums) {
    int numPartitions = partitionChecksums.length;
    long[] checksumValues = new long[numPartitions];
    for (int i = 0; i < numPartitions; i ++) {
      checksumValues[i] = partitionChecksums[i].getValue();
    }
    return checksumValues;
  }

  public static String shuffleChecksumAlgorithm(SparkConf conf) {
    return conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ALGORITHM());
  }

  public static String getChecksumFileName(ShuffleChecksumBlockId blockId, SparkConf conf) {
    // append the shuffle checksum algorithm as the file extension
    return String.format("%s.%s", blockId.name(), shuffleChecksumAlgorithm(conf));
  }
}
