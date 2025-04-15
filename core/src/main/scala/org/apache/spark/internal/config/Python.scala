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

private[spark] object Python {
  val PYTHON_WORKER_REUSE = ConfigBuilder("spark.python.worker.reuse")
    .version("1.2.0")
    .booleanConf
    .createWithDefault(true)

  val PYTHON_TASK_KILL_TIMEOUT = ConfigBuilder("spark.python.task.killTimeout")
    .version("2.2.2")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("2s")

  val PYTHON_USE_DAEMON = ConfigBuilder("spark.python.use.daemon")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(true)

  val PYTHON_LOG_INFO = ConfigBuilder("spark.executor.python.worker.log.details")
    .version("3.5.0")
    .booleanConf
    .createWithDefault(false)

  val PYTHON_DAEMON_MODULE = ConfigBuilder("spark.python.daemon.module")
    .version("2.4.0")
    .stringConf
    .createOptional

  val PYTHON_WORKER_MODULE = ConfigBuilder("spark.python.worker.module")
    .version("2.4.0")
    .stringConf
    .createOptional

  val PYSPARK_EXECUTOR_MEMORY = ConfigBuilder("spark.executor.pyspark.memory")
    .version("2.4.0")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  val PYTHON_AUTH_SOCKET_TIMEOUT = ConfigBuilder("spark.python.authenticate.socketTimeout")
    .internal()
    .version("3.1.0")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("15s")

  val PYTHON_WORKER_FAULTHANLDER_ENABLED = ConfigBuilder("spark.python.worker.faulthandler.enabled")
    .doc("When true, Python workers set up the faulthandler for the case when the Python worker " +
      "exits unexpectedly (crashes), and shows the stack trace of the moment the Python worker " +
      "crashes in the error message if captured successfully.")
    .version("3.2.0")
    .booleanConf
    .createWithDefault(false)

  val PYTHON_UNIX_DOMAIN_SOCKET_ENABLED = ConfigBuilder("spark.python.unix.domain.socket.enabled")
    .doc("When set to true, the Python driver uses a Unix domain socket for operations like " +
      "creating or collecting a DataFrame from local data, using accumulators, and executing " +
      "Python functions with PySpark such as Python UDFs. This configuration only applies " +
      "to Spark Classic and Spark Connect server.")
    .version("4.1.0")
    .booleanConf
    .createWithDefault(sys.env.get("PYSPARK_UDS_MODE").contains("true"))

  val PYTHON_UNIX_DOMAIN_SOCKET_DIR = ConfigBuilder("spark.python.unix.domain.socket.dir")
    .doc("When specified, it uses the directory to create Unix domain socket files. " +
      "Otherwise, it uses the default location of the temporary directory set in " +
      s"'java.io.tmpdir' property. This is used when ${PYTHON_UNIX_DOMAIN_SOCKET_ENABLED.key} " +
      "is enabled.")
    .internal()
    .version("4.1.0")
    .stringConf
    // UDS requires the length of path lower than 104 characters. We use UUID (36 characters)
    // and additional prefix "." (1), postfix ".sock" (5), and the path separator (1).
    .checkValue(
      _.length <= (104 - (36 + 1 + 5 + 1)),
      s"The directory path should be lower than ${(104 - (36 + 1 + 5 + 1))}")
    .createOptional

  private val PYTHON_WORKER_IDLE_TIMEOUT_SECONDS_KEY = "spark.python.worker.idleTimeoutSeconds"
  private val PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT_KEY = "spark.python.worker.killOnIdleTimeout"

  val PYTHON_WORKER_IDLE_TIMEOUT_SECONDS = ConfigBuilder(PYTHON_WORKER_IDLE_TIMEOUT_SECONDS_KEY)
    .doc("The time (in seconds) Spark will wait for activity " +
      "(e.g., data transfer or communication) from a Python worker before considering it " +
      "potentially idle or unresponsive. When the timeout is triggered, " +
      "Spark will log the network-related status for debugging purposes. " +
      "However, the Python worker will remain active and continue waiting for communication " +
      s"unless explicitly terminated via $PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT_KEY." +
      "The default is `0` that means no timeout.")
    .version("4.0.0")
    .timeConf(TimeUnit.SECONDS)
    .checkValue(_ >= 0, "The idle timeout should be 0 or positive.")
    .createWithDefault(0)

  val PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT = ConfigBuilder(PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT_KEY)
    .doc("Whether Spark should terminate the Python worker process when the idle timeout " +
      s"(as defined by $PYTHON_WORKER_IDLE_TIMEOUT_SECONDS_KEY) is reached. If enabled, " +
      "Spark will terminate the Python worker process in addition to logging the status.")
    .version("4.1.0")
    .booleanConf
    .createWithDefault(false)
}
