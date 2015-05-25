package org.apache.spark.deploy.yarn

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.deploy.yarn.YarnResourceUsage

/**
 *
 * @param appId -- application Id
 * @param usage  -- Yarn Resource Usage
 * @param progress --
 * */
case class YarnAppProgress(appId: ApplicationId,
                           trackingUrl: String,
                           usage: YarnResourceUsage,
                           progress: Float = 0)
