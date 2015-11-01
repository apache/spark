package org.apache.spark.network.yarn

import org.apache.spark.SSLSampleConfigs

/**
 *
 */
class SslYarnShuffleServiceSuite extends YarnShuffleServiceSuite {

  /**
   * Override to add "spark.ssl.bts.*" configuration parameters...
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    SSLSampleConfigs.setSparkSSLShuffleConfig(yarnConfig)
  }
}
