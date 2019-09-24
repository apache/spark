/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.internal.config

import java.util.concurrent.TimeUnit

import org.apache.spark.network.util.ByteUnit

private[spark] object UI {

  val UI_SHOW_CONSOLE_PROGRESS = ConfigBuilder("spark.ui.showConsoleProgress")
    .doc("When true, show the progress bar in the console.")
    .booleanConf
    .createWithDefault(false)

  val UI_CONSOLE_PROGRESS_UPDATE_INTERVAL =
    ConfigBuilder("spark.ui.consoleProgress.update.interval")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(200)

  val UI_ENABLED = ConfigBuilder("spark.ui.enabled")
    .doc("Whether to run the web UI for the Spark application.")
    .booleanConf
    .createWithDefault(true)

  val UI_PORT = ConfigBuilder("spark.ui.port")
    .doc("Port for your application's dashboard, which shows memory and workload data.")
    .intConf
    .createWithDefault(4040)

  val UI_FILTERS = ConfigBuilder("spark.ui.filters")
    .doc("Comma separated list of filter class names to apply to the Spark Web UI.")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val UI_ALLOW_FRAMING_FROM = ConfigBuilder("spark.ui.allowFramingFrom")
    .stringConf
    .createOptional

  val UI_REVERSE_PROXY = ConfigBuilder("spark.ui.reverseProxy")
    .doc("Enable running Spark Master as reverse proxy for worker and application UIs. " +
      "In this mode, Spark master will reverse proxy the worker and application UIs to enable " +
      "access without requiring direct access to their hosts. Use it with caution, as worker " +
      "and application UI will not be accessible directly, you will only be able to access them" +
      "through spark master/proxy public URL. This setting affects all the workers and " +
      "application UIs running in the cluster and must be set on all the workers, drivers " +
      " and masters.")
    .booleanConf
    .createWithDefault(false)

  val UI_REVERSE_PROXY_URL = ConfigBuilder("spark.ui.reverseProxyUrl")
    .doc("This is the URL where your proxy is running. This URL is for proxy which is running " +
      "in front of Spark Master. This is useful when running proxy for authentication e.g. " +
      "OAuth proxy. Make sure this is a complete URL including scheme (http/https) and port to " +
      "reach your proxy.")
    .stringConf
    .createOptional

  val UI_KILL_ENABLED = ConfigBuilder("spark.ui.killEnabled")
    .doc("Allows jobs and stages to be killed from the web UI.")
    .booleanConf
    .createWithDefault(true)

  val UI_THREAD_DUMPS_ENABLED = ConfigBuilder("spark.ui.threadDumpsEnabled")
    .booleanConf
    .createWithDefault(true)

  val UI_X_XSS_PROTECTION = ConfigBuilder("spark.ui.xXssProtection")
    .doc("Value for HTTP X-XSS-Protection response header")
    .stringConf
    .createWithDefaultString("1; mode=block")

  val UI_X_CONTENT_TYPE_OPTIONS = ConfigBuilder("spark.ui.xContentTypeOptions.enabled")
    .doc("Set to 'true' for setting X-Content-Type-Options HTTP response header to 'nosniff'")
    .booleanConf
    .createWithDefault(true)

  val UI_STRICT_TRANSPORT_SECURITY = ConfigBuilder("spark.ui.strictTransportSecurity")
    .doc("Value for HTTP Strict Transport Security Response Header")
    .stringConf
    .createOptional

  val UI_REQUEST_HEADER_SIZE = ConfigBuilder("spark.ui.requestHeaderSize")
    .doc("Value for HTTP request header size in bytes.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("8k")

  val UI_TIMELINE_TASKS_MAXIMUM = ConfigBuilder("spark.ui.timeline.tasks.maximum")
    .intConf
    .createWithDefault(1000)

  val ACLS_ENABLE = ConfigBuilder("spark.acls.enable")
    .booleanConf
    .createWithDefault(false)

  val UI_VIEW_ACLS = ConfigBuilder("spark.ui.view.acls")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val UI_VIEW_ACLS_GROUPS = ConfigBuilder("spark.ui.view.acls.groups")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val ADMIN_ACLS = ConfigBuilder("spark.admin.acls")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val ADMIN_ACLS_GROUPS = ConfigBuilder("spark.admin.acls.groups")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val MODIFY_ACLS = ConfigBuilder("spark.modify.acls")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val MODIFY_ACLS_GROUPS = ConfigBuilder("spark.modify.acls.groups")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val USER_GROUPS_MAPPING = ConfigBuilder("spark.user.groups.mapping")
    .stringConf
    .createWithDefault("org.apache.spark.security.ShellBasedGroupsMappingProvider")

  val CUSTOM_EXECUTOR_LOG_URL = ConfigBuilder("spark.ui.custom.executor.log.url")
    .doc("Specifies custom spark executor log url for supporting external log service instead of " +
      "using cluster managers' application log urls in the Spark UI. Spark will support " +
      "some path variables via patterns which can vary on cluster manager. Please check the " +
      "documentation for your cluster manager to see which patterns are supported, if any. " +
      "This configuration replaces original log urls in event log, which will be also effective " +
      "when accessing the application on history server. The new log urls must be permanent, " +
      "otherwise you might have dead link for executor log urls.")
    .stringConf
    .createOptional
}
