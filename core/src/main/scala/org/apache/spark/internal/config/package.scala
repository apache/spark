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

package org.apache.spark.internal

import java.util.concurrent.TimeUnit

import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.util.Utils

package object config {

  private[spark] val DRIVER_CLASS_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_CLASSPATH)
      .doc("Extra classpath entries to prepend to the classpath of the driver.")
      .stringConf.createOptional

  private[spark] val DRIVER_JAVA_OPTIONS =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)
      .doc("A string of extra JVM options to pass to the driver. For instance, " +
        "GC settings or other logging.")
      .stringConf.createOptional

  private[spark] val DRIVER_LIBRARY_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH)
      .doc("Set a special library path to use when launching the driver JVM.")
      .stringConf.createOptional

  private[spark] val DRIVER_USER_CLASS_PATH_FIRST =
    ConfigBuilder("spark.driver.userClassPathFirst")
      .doc("Whether to give user-added jars precedence over Spark's own jars " +
        "when loading classes in the driver. This feature can be used to mitigate " +
        "conflicts between Spark's dependencies and user dependencies. " +
        "It is currently an experimental feature. This is used in cluster mode only.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val DRIVER_MEMORY = ConfigBuilder("spark.driver.memory")
    .doc("Amount of memory to use for the driver process, i.e. " +
      "where SparkContext is initialized. (e.g. 512m, 1g, 2g).")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("1g")

  private[spark] val EXECUTOR_CLASS_PATH =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH)
      .doc("Extra classpath entries to prepend to the classpath of executors. " +
        "This exists primarily for backwards-compatibility with older versions of Spark. " +
        "Users typically should not need to set this option.")
      .stringConf.createOptional

  private[spark] val EXECUTOR_JAVA_OPTIONS =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS)
      .doc("A string of extra JVM options to pass to executors. For instance, " +
        "GC settings or other logging.")
      .stringConf.createOptional

  private[spark] val EXECUTOR_LIBRARY_PATH =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_LIBRARY_PATH)
      .doc("Set a special library path to use when launching executor JVM's.")
      .stringConf.createOptional

  private[spark] val EXECUTOR_USER_CLASS_PATH_FIRST =
    ConfigBuilder("spark.executor.userClassPathFirst")
      .doc("Same functionality as spark.driver.userClassPathFirst, " +
        "but applied to executor instances.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EXECUTOR_MEMORY = ConfigBuilder("spark.executor.memory")
    .doc("Amount of memory to use per executor process (e.g. 512m, 1g, 2g, 8g).")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("1g")

  private[spark] val IS_PYTHON_APP = ConfigBuilder("spark.yarn.isPython").internal()
    .booleanConf.createWithDefault(false)

  private[spark] val CPUS_PER_TASK = ConfigBuilder("spark.task.cpus")
    .doc("Number of cores to allocate for each task.")
    .intConf
    .createWithDefault(1)

  private[spark] val DYN_ALLOCATION_MIN_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.minExecutors")
      .doc("Lower bound for the number of executors if dynamic allocation is enabled.")
      .intConf
      .createWithDefault(0)

  private[spark] val DYN_ALLOCATION_INITIAL_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.initialExecutors")
      .doc("Initial number of executors to run if dynamic allocation is enabled. " +
        "If `--num-executors` (or `spark.executor.instances`) is set " +
        "and larger than this value, " +
        "it will be used as the initial number of executors.")
      .fallbackConf(DYN_ALLOCATION_MIN_EXECUTORS)

  private[spark] val DYN_ALLOCATION_MAX_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.maxExecutors")
      .doc("Upper bound for the number of executors if dynamic allocation is enabled.")
      .intConf
      .createWithDefault(Int.MaxValue)

  private[spark] val SHUFFLE_SERVICE_ENABLED =
    ConfigBuilder("spark.shuffle.service.enabled")
      .doc("Enables the external shuffle service. This service preserves " +
        "the shuffle files written by executors so the executors can be safely removed. " +
        "This must be enabled if spark.dynamicAllocation.enabled is 'true'. " +
        "The external shuffle service must be set up in order to enable it. " +
        "See dynamic allocation configuration and setup documentation for more information.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val KEYTAB = ConfigBuilder("spark.yarn.keytab")
    .doc("Location of user's keytab.")
    .stringConf.createOptional

  private[spark] val PRINCIPAL = ConfigBuilder("spark.yarn.principal")
    .doc("Name of the Kerberos principal.")
    .stringConf.createOptional

  private[spark] val EXECUTOR_INSTANCES = ConfigBuilder("spark.executor.instances")
    .intConf
    .createOptional

  private[spark] val PY_FILES = ConfigBuilder("spark.submit.pyFiles")
    .doc("Comma-separated list of .zip, .egg, or .py files to place on " +
      "the PYTHONPATH for Python apps.")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val MAX_TASK_FAILURES =
    ConfigBuilder("spark.task.maxFailures")
      .doc("Number of failures of any particular task before giving up on the job. " +
        "The total number of failures spread across different tasks " +
        "will not cause the job to fail; " +
        "a particular task has to fail this number of attempts. " +
        "Should be greater than or equal to 1. Number of allowed retries = this value - 1.")
      .intConf
      .createWithDefault(4)

  // Blacklist confs
  private[spark] val BLACKLIST_ENABLED =
    ConfigBuilder("spark.blacklist.enabled")
      .doc("If set to 'true', prevent Spark from scheduling tasks on executors " +
        "that have been blacklisted due to too many task failures. " +
        "The blacklisting algorithm can be further controlled by " +
        "the other 'spark.blacklist' configuration options.")
      .booleanConf
      .createOptional

  private[spark] val MAX_TASK_ATTEMPTS_PER_EXECUTOR =
    ConfigBuilder("spark.blacklist.task.maxTaskAttemptsPerExecutor")
      .doc("For a given task, how many times it can be " +
        "retried on one executor before the executor is blacklisted for that task.")
      .intConf
      .createWithDefault(1)

  private[spark] val MAX_TASK_ATTEMPTS_PER_NODE =
    ConfigBuilder("spark.blacklist.task.maxTaskAttemptsPerNode")
      .doc("For a given task, how many times it can be " +
        "retried on one node, before the entire node is blacklisted for that task.")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILURES_PER_EXEC =
    ConfigBuilder("spark.blacklist.application.maxFailedTasksPerExecutor")
      .doc("(Experimental) How many different tasks must fail on one executor, " +
        "in successful task sets, before the executor is blacklisted for the entire application. " +
        "Blacklisted executors will be automatically added back to the pool of available " +
        "resources after the timeout specified by spark.blacklist.timeout. " +
        "Note that with dynamic allocation, though, the executors may get marked " +
        "as idle and be reclaimed by the cluster manager.")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILURES_PER_EXEC_STAGE =
    ConfigBuilder("spark.blacklist.stage.maxFailedTasksPerExecutor")
      .doc("How many different tasks must fail on one executor, " +
        "within one stage, before the executor is blacklisted for that stage.")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILED_EXEC_PER_NODE =
    ConfigBuilder("spark.blacklist.application.maxFailedExecutorsPerNode")
      .doc("(Experimental) How many different executors must be blacklisted for " +
        "the entire application, before the node is blacklisted for the entire application. " +
        "Blacklisted nodes will be automatically added back to the pool of available resources " +
        "after the timeout specified by spark.blacklist.timeout. " +
        "Note that with dynamic allocation, though, the executors on the node may get marked " +
        "as idle and be reclaimed by the cluster manager.")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILED_EXEC_PER_NODE_STAGE =
    ConfigBuilder("spark.blacklist.stage.maxFailedExecutorsPerNode")
      .doc("How many different executors are marked as blacklisted for a given stage, " +
        "before the entire node is marked as failed for the stage.")
      .intConf
      .createWithDefault(2)

  private[spark] val BLACKLIST_TIMEOUT_CONF =
    ConfigBuilder("spark.blacklist.timeout")
      .doc("(Experimental) How long a node or executor is blacklisted for " +
        "the entire application, before it is unconditionally removed from " +
        "the blacklist to attempt running new tasks.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val BLACKLIST_KILL_ENABLED =
    ConfigBuilder("spark.blacklist.killBlacklistedExecutors")
      .doc("(Experimental) If set to true, allow Spark to automatically kill, " +
        "and attempt to re-create, executors when they are blacklisted. Note that, " +
        "when an entire node is added to the blacklist, all of the executors " +
        "on that node will be killed.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val BLACKLIST_LEGACY_TIMEOUT_CONF =
    ConfigBuilder("spark.scheduler.executorTaskBlacklistTime")
      .internal()
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val BLACKLIST_FETCH_FAILURE_ENABLED =
    ConfigBuilder("spark.blacklist.application.fetchFailure.enabled")
      .booleanConf
      .createWithDefault(false)
  // End blacklist confs

  private[spark] val UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE =
    ConfigBuilder("spark.files.fetchFailure.unRegisterOutputOnHost")
      .doc("Whether to un-register all the outputs on the host in condition that we receive " +
        " a FetchFailure. This is set default to false, which means, we only un-register the " +
        " outputs related to the exact executor(instead of the host) on a FetchFailure.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val LISTENER_BUS_EVENT_QUEUE_CAPACITY =
    ConfigBuilder("spark.scheduler.listenerbus.eventqueue.capacity")
      .withAlternative("spark.scheduler.listenerbus.eventqueue.size")
      .intConf
      .checkValue(_ > 0, "The capacity of listener bus event queue must not be negative")
      .createWithDefault(10000)

  private[spark] val LISTENER_BUS_METRICS_MAX_LISTENER_CLASSES_TIMED =
    ConfigBuilder("spark.scheduler.listenerbus.metrics.maxListenerClassesTimed")
      .internal()
      .intConf
      .createWithDefault(128)

  // This property sets the root namespace for metrics reporting
  private[spark] val METRICS_NAMESPACE = ConfigBuilder("spark.metrics.namespace")
    .stringConf
    .createOptional

  private[spark] val PYSPARK_DRIVER_PYTHON = ConfigBuilder("spark.pyspark.driver.python")
    .doc("Python binary executable to use for PySpark in driver.")
    .stringConf
    .createOptional

  private[spark] val PYSPARK_PYTHON = ConfigBuilder("spark.pyspark.python")
    .doc("Python binary executable to use for PySpark in both driver and executors.")
    .stringConf
    .createOptional

  // To limit memory usage, we only track information for a fixed number of tasks
  private[spark] val UI_RETAINED_TASKS = ConfigBuilder("spark.ui.retainedTasks")
    .doc("How many tasks the Spark UI and status APIs remember before garbage collecting.")
    .intConf
    .createWithDefault(100000)

  // To limit how many applications are shown in the History Server summary ui
  private[spark] val HISTORY_UI_MAX_APPS =
    ConfigBuilder("spark.history.ui.maxApplications").intConf.createWithDefault(Integer.MAX_VALUE)

  private[spark] val IO_ENCRYPTION_ENABLED = ConfigBuilder("spark.io.encryption.enabled")
    .doc("Enable IO encryption. Currently supported by all modes except Mesos. " +
      "It's recommended that RPC encryption be enabled when using this feature.")
    .booleanConf
    .createWithDefault(false)

  private[spark] val IO_ENCRYPTION_KEYGEN_ALGORITHM =
    ConfigBuilder("spark.io.encryption.keygen.algorithm")
      .doc("The algorithm to use when generating the IO encryption key. " +
        "The supported algorithms are described in the KeyGenerator section of " +
        "the Java Cryptography Architecture Standard Algorithm Name Documentation.")
      .stringConf
      .createWithDefault("HmacSHA1")

  private[spark] val IO_ENCRYPTION_KEY_SIZE_BITS = ConfigBuilder("spark.io.encryption.keySizeBits")
    .doc("IO encryption key size in bits. Supported values are 128, 192 and 256.")
    .intConf
    .checkValues(Set(128, 192, 256))
    .createWithDefault(128)

  private[spark] val IO_CRYPTO_CIPHER_TRANSFORMATION =
    ConfigBuilder("spark.io.crypto.cipher.transformation")
      .internal()
      .stringConf
      .createWithDefaultString("AES/CTR/NoPadding")

  private[spark] val DRIVER_HOST_ADDRESS = ConfigBuilder("spark.driver.host")
    .doc("Address of driver endpoints.")
    .stringConf
    .createWithDefault(Utils.localHostName())

  private[spark] val DRIVER_BIND_ADDRESS = ConfigBuilder("spark.driver.bindAddress")
    .doc("Address where to bind network listen sockets on the driver.")
    .fallbackConf(DRIVER_HOST_ADDRESS)

  private[spark] val BLOCK_MANAGER_PORT = ConfigBuilder("spark.blockManager.port")
    .doc("Port to use for the block manager when a more specific setting is not provided.")
    .intConf
    .createWithDefault(0)

  private[spark] val DRIVER_BLOCK_MANAGER_PORT = ConfigBuilder("spark.driver.blockManager.port")
    .doc("Port to use for the block manager on the driver.")
    .fallbackConf(BLOCK_MANAGER_PORT)

  private[spark] val IGNORE_CORRUPT_FILES = ConfigBuilder("spark.files.ignoreCorruptFiles")
    .doc("Whether to ignore corrupt files. If true, the Spark jobs will continue to run when " +
      "encountering corrupted or non-existing files and contents that have been read will still " +
      "be returned.")
    .booleanConf
    .createWithDefault(false)

  private[spark] val APP_CALLER_CONTEXT = ConfigBuilder("spark.log.callerContext")
    .doc("Application information that will be written into Yarn RM log/HDFS audit log " +
      "when running on Yarn/HDFS. Its length depends on the Hadoop configuration " +
      "hadoop.caller.context.max.size. It should be concise, " +
      "and typically can have up to 50 characters.")
    .stringConf
    .createOptional

  private[spark] val FILES_MAX_PARTITION_BYTES = ConfigBuilder("spark.files.maxPartitionBytes")
    .doc("The maximum number of bytes to pack into a single partition when reading files.")
    .longConf
    .createWithDefault(128 * 1024 * 1024)

  private[spark] val FILES_OPEN_COST_IN_BYTES = ConfigBuilder("spark.files.openCostInBytes")
    .doc("The estimated cost to open a file, measured by the number of bytes could be scanned in" +
      " the same time. This is used when putting multiple files into a partition. It's better to" +
      " over estimate, then the partitions with small files will be faster than partitions with" +
      " bigger files.")
    .longConf
    .createWithDefault(4 * 1024 * 1024)

  private[spark] val SECRET_REDACTION_PATTERN =
    ConfigBuilder("spark.redaction.regex")
      .doc("Regex to decide which Spark configuration properties and environment variables in " +
        "driver and executor environments contain sensitive information. When this regex matches " +
        "a property key or value, the value is redacted from the environment UI and various logs " +
        "like YARN and event logs.")
      .regexConf
      .createWithDefault("(?i)secret|password".r)

  private[spark] val STRING_REDACTION_PATTERN =
    ConfigBuilder("spark.redaction.string.regex")
      .doc("Regex to decide which parts of strings produced by Spark contain sensitive " +
        "information. When this regex matches a string part, that string part is replaced by a " +
        "dummy value. This is currently used to redact the output of SQL explain commands.")
      .regexConf
      .createOptional

  private[spark] val NETWORK_AUTH_ENABLED =
    ConfigBuilder("spark.authenticate")
      .doc("Whether Spark authenticates its internal connections. " +
        "See spark.authenticate.secret if not running on YARN.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SASL_ENCRYPTION_ENABLED =
    ConfigBuilder("spark.authenticate.enableSaslEncryption")
      .doc("Enable encrypted communication when authentication is enabled. " +
        "This is supported by the block transfer service and the RPC endpoints.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val NETWORK_ENCRYPTION_ENABLED =
    ConfigBuilder("spark.network.crypto.enabled")
      .doc("Enable encryption using the commons-crypto library for RPC and " +
        "block transfer service. Requires spark.authenticate to be enabled.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val CHECKPOINT_COMPRESS =
    ConfigBuilder("spark.checkpoint.compress")
      .doc("Whether to compress RDD checkpoints. Generally a good idea. Compression will use " +
        "spark.io.compression.codec.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_ACCURATE_BLOCK_THRESHOLD =
    ConfigBuilder("spark.shuffle.accurateBlockThreshold")
      .doc("When we compress the size of shuffle blocks in HighlyCompressedMapStatus, we will " +
        "record the size accurately if it's above this config. This helps to prevent OOM by " +
        "avoiding underestimating shuffle block size when fetch shuffle blocks.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(100 * 1024 * 1024)

  private[spark] val SHUFFLE_REGISTRATION_TIMEOUT =
    ConfigBuilder("spark.shuffle.registration.timeout")
      .doc("Timeout in milliseconds for registration to the external shuffle service.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(5000)

  private[spark] val SHUFFLE_REGISTRATION_MAX_ATTEMPTS =
    ConfigBuilder("spark.shuffle.registration.maxAttempts")
      .doc("When we fail to register to the external shuffle service, we will " +
        "retry for maxAttempts times.")
      .intConf
      .createWithDefault(3)

  private[spark] val REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS =
    ConfigBuilder("spark.reducer.maxBlocksInFlightPerAddress")
      .doc("This configuration limits the number of remote blocks being fetched per reduce task" +
        " from a given host port. When a large number of blocks are being requested from a given" +
        " address in a single fetch or simultaneously, this could crash the serving executor or" +
        " Node Manager. This is especially useful to reduce the load on the Node Manager when" +
        " external shuffle is enabled. You can mitigate the issue by setting it to a lower value.")
      .intConf
      .checkValue(_ > 0, "The max no. of blocks in flight cannot be non-positive.")
      .createWithDefault(Int.MaxValue)

  private[spark] val REDUCER_MAX_REQ_SIZE_SHUFFLE_TO_MEM =
    ConfigBuilder("spark.reducer.maxReqSizeShuffleToMem")
      .doc("The blocks of a shuffle request will be fetched to disk when size of the request is " +
        "above this threshold. This is to avoid a giant request takes too much memory. We can " +
        "enable this config by setting a specific value(e.g. 200m). Note that this config can " +
        "be enabled only when the shuffle shuffle service is newer than Spark-2.2 or the shuffle" +
        " service is disabled.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(Long.MaxValue)

  private[spark] val TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES =
    ConfigBuilder("spark.taskMetrics.trackUpdatedBlockStatuses")
      .doc("Enable tracking of updatedBlockStatuses in the TaskMetrics. Off by default since " +
        "tracking the block statuses can use a lot of memory and its not used anywhere within " +
        "spark.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_FILE_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.file.buffer")
      .doc("Size of the in-memory buffer for each shuffle file output stream. " +
        "These buffers reduce the number of disk seeks and system calls made " +
        "in creating intermediate shuffle files.")
      .bytesConf(ByteUnit.KiB)
      .checkValue(v => v > 0 && v <= Int.MaxValue / 1024,
        s"The file buffer size must be greater than 0 and less than ${Int.MaxValue / 1024}.")
      .createWithDefaultString("32k")

  private[spark] val SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.unsafe.file.output.buffer")
      .doc("The file system for this buffer size after each partition " +
        "is written in unsafe shuffle writer.")
      .bytesConf(ByteUnit.KiB)
      .checkValue(v => v > 0 && v <= Int.MaxValue / 1024,
        s"The buffer size must be greater than 0 and less than ${Int.MaxValue / 1024}.")
      .createWithDefaultString("32k")

  private[spark] val SHUFFLE_DISK_WRITE_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.spill.diskWriteBufferSize")
      .doc("The buffer size to use when writing the sorted records to an on-disk file.")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v > 0 && v <= Int.MaxValue,
        s"The buffer size must be greater than 0 and less than ${Int.MaxValue}.")
      .createWithDefault(1024 * 1024)
}
