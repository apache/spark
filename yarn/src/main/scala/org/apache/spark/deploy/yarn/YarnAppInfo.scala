package org.apache.spark.deploy.yarn

import org.apache.hadoop.yarn.api.records.ApplicationId

case class YarnAppInfo(appId: ApplicationId,
                       user: String,
                       queue: String,
                       name: String,
                       masterHost: String,
                       masterRpcPort: Int,
                       state: String,
                       diagnostics: String,
                       trackingUrl: String,
                       startTime: Long)
