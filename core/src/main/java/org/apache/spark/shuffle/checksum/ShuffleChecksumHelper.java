package org.apache.spark.shuffle.checksum;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.internal.config.package$;

import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class ShuffleChecksumHelper {

  public static boolean isShuffleChecksumEnabled(SparkConf conf) {
    return (boolean) conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ENABLED());
  }

  public static Checksum[] createPartitionChecksums(int numPartitions, SparkConf conf)
    throws SparkException {
    Checksum[] partitionChecksums;
    String checksumAlgo = conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ALGORITHM());
    switch (checksumAlgo) {
      case "Adler32":
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
}
