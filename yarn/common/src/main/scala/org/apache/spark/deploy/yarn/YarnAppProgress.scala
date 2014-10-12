package org.apache.spark.deploy.yarn

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.deploy.yarn.YarnResourceUsage

/**
 *
 * @param appId -- application Id
 * @param trackingUrl -- tracking URL
 * @param usage  -- Yarn Resource Usage
 * @param progress -- note: for Yarn-Alpha, no progress is reported, so the value will be always default value.
 * */
 case class YarnAppProgress(appId: ApplicationId,
                           trackingUrl: String,
                           usage: YarnResourceUsage,
                           progress: Float = 0)
