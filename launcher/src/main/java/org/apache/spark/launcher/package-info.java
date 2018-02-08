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

/**
 * Library for launching Spark applications programmatically.
 *
 * <p>
 * There are two ways to start applications with this library: as a child process, using
 * {@link org.apache.spark.launcher.SparkLauncher}, or in-process, using
 * {@link org.apache.spark.launcher.InProcessLauncher}.
 * </p>
 *
 * <p>
 * The {@link org.apache.spark.launcher.AbstractLauncher#startApplication(
 * org.apache.spark.launcher.SparkAppHandle.Listener...)}  method can be used to start Spark and
 * provide a handle to monitor and control the running application:
 * </p>
 *
 * <pre>
 * {@code
 *   import org.apache.spark.launcher.SparkAppHandle;
 *   import org.apache.spark.launcher.SparkLauncher;
 *
 *   public class MyLauncher {
 *     public static void main(String[] args) throws Exception {
 *       SparkAppHandle handle = new SparkLauncher()
 *         .setAppResource("/my/app.jar")
 *         .setMainClass("my.spark.app.Main")
 *         .setMaster("local")
 *         .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
 *         .startApplication();
 *       // Use handle API to monitor / control application.
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>
 * Launching applications as a child process requires a full Spark installation. The installation
 * directory can be provided to the launcher explicitly in the launcher's configuration, or by
 * setting the <i>SPARK_HOME</i> environment variable.
 * </p>
 *
 * <p>
 * Launching applications in-process is only recommended in cluster mode, since Spark cannot run
 * multiple client-mode applications concurrently in the same process. The in-process launcher
 * requires the necessary Spark dependencies (such as spark-core and cluster manager-specific
 * modules) to be present in the caller thread's class loader.
 * </p>
 *
 * <p>
 * It's also possible to launch a raw child process, without the extra monitoring, using the
 * {@link org.apache.spark.launcher.SparkLauncher#launch()} method:
 * </p>
 *
 * <pre>
 * {@code
 *   import org.apache.spark.launcher.SparkLauncher;
 *
 *   public class MyLauncher {
 *     public static void main(String[] args) throws Exception {
 *       Process spark = new SparkLauncher()
 *         .setAppResource("/my/app.jar")
 *         .setMainClass("my.spark.app.Main")
 *         .setMaster("local")
 *         .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
 *         .launch();
 *       spark.waitFor();
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>This method requires the calling code to manually manage the child process, including its
 * output streams (to avoid possible deadlocks). It's recommended that
 * {@link org.apache.spark.launcher.SparkLauncher#startApplication(
 *   org.apache.spark.launcher.SparkAppHandle.Listener...)} be used instead.</p>
 */
package org.apache.spark.launcher;
