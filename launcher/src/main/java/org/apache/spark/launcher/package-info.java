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
 * <p/>
 * This library allows applications to launch Spark programmatically. There's only one entry
 * point to the library - the {@link org.apache.spark.launcher.SparkLauncher} class.
 * <p/>
 * To launch a Spark application, just instantiate a {@link org.apache.spark.launcher.SparkLauncher}
 * and configure the application to run. For example:
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
 */
package org.apache.spark.launcher;
