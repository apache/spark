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

package org.apache.spark.launcher;

/**
 * This helper class is used to place some JVM runtime options(eg: `--add-opens`)
 * required by Spark when using Java 17. `DEFAULT_MODULE_OPTIONS` has added
 * `-XX:+IgnoreUnrecognizedVMOptions` to be robust.
 *
 * @since 3.3.0
 */
public class JavaModuleOptions {
    private static final String[] DEFAULT_MODULE_OPTIONS = {
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-modules=jdk.incubator.vector",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "-Djdk.reflect.useDirectMethodHandle=false",
      "-Dio.netty.tryReflectionSetAccessible=true"};

    /**
     * Returns the default JVM runtime options used by Spark.
     */
    public static String defaultModuleOptions() {
      return String.join(" ", DEFAULT_MODULE_OPTIONS);
    }

    /**
     * Returns the default JVM runtime option array used by Spark.
     */
    public static String[] defaultModuleOptionArray() {
      return DEFAULT_MODULE_OPTIONS;
    }
}
