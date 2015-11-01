package org.apache.spark

/**
 *
 */
class SslShuffleNettySuite extends ShuffleNettySuite {

  override def beforeAll() {
    conf.set("spark.shuffle.blockTransferService", "netty")
    SSLSampleConfigs.setSparkSSLShuffleConfig(conf)
  }
}
