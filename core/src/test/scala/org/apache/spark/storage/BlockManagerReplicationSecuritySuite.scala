package org.apache.spark.storage

import org.apache.spark.SSLSampleConfigs._
import org.apache.spark.SparkConf

/**
 *
 */
class BlockManagerReplicationSecuritySuite extends BlockManagerReplicationSuite {

  /**
   * Create a [[SparkConf]] with the appropriate SSL settings...
   * @return
   */
  override private[storage] def createConf(): SparkConf = {
    sparkSSLConfig().set("spark.app.id", "test")
  }
}
