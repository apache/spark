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
 * Library for launching Spark applications.
 *
 * <p>
 * This library allows applications to launch Spark programmatically. There's only one entry
 * point to the library - the {@link org.apache.spark.launcher.SparkLauncher} class.
 * </p>
 *
 * <p>
 * The {@link org.apache.spark.launcher.SparkLauncher#startApplication(
 * org.apache.spark.launcher.SparkAppHandle.Listener...)} can be used to start Spark and provide
 * a handle to monitor and control the running application:
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
 * Currently, for currently while launching spark application with
 * {@link org.apache.spark.launcher.SparkLauncher#startApplication(
 * org.apache.spark.launcher.SparkAppHandle.Listener...)}, there are two options available
 * for YARN manager in cluster deploy mode:
 *  - to launch Spark Application as a Thread inside current JVM using
 *    the {@link org.apache.spark.launcher.SparkLauncher#launchSparkSubmitAsThread(boolean)}
 *  - to request application be killed if launcher process exits using
 *    the {@link org.apache.spark.launcher.SparkLauncher#stopIfInterrupted()}.
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
 *         .setMaster("yarn")
 *         .stopIfInterrupted()
 *         .launchSparkSubmitAsThread(true)
 *         .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
 *         .startApplication();
 *       // Use handle API to monitor / control application.
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>
 * It's also possible to launch a raw child process, using the
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
