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

package org.apache.spark.sql.hive

/** Support for interacting with different versions of the HiveMetastoreClient */
package object client {
  private[hive] sealed abstract class HiveVersion(
      val fullVersion: String,
      val extraDeps: Seq[String] = Nil,
      val exclusions: Seq[String] = Nil)

  // scalastyle:off
  private[hive] object hive {
    case object v12 extends HiveVersion("0.12.0")
    case object v13 extends HiveVersion("0.13.1")

    // Hive 0.14 depends on calcite 0.9.2-incubating-SNAPSHOT which does not exist in
    // maven central anymore, so override those with a version that exists.
    //
    // The other excluded dependencies are also nowhere to be found, so exclude them explicitly. If
    // they're needed by the metastore client, users will have to dig them out of somewhere and use
    // configuration to point Spark at the correct jars.
    case object v14 extends HiveVersion("0.14.0",
      extraDeps = Seq("org.apache.calcite:calcite-core:1.3.0-incubating",
        "org.apache.calcite:calcite-avatica:1.3.0-incubating"),
      exclusions = Seq("org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v1_0 extends HiveVersion("1.0.0",
      exclusions = Seq("eigenbase:eigenbase-properties",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "net.hydromatic:linq4j",
        "net.hydromatic:quidem"))

    // The curator dependency was added to the exclusions here because it seems to confuse the ivy
    // library. org.apache.curator:curator is a pom dependency but ivy tries to find the jar for it,
    // and fails.
    case object v1_1 extends HiveVersion("1.1.0",
      exclusions = Seq("eigenbase:eigenbase-properties",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "net.hydromatic:linq4j",
        "net.hydromatic:quidem"))

    case object v1_2 extends HiveVersion("1.2.1",
      exclusions = Seq("eigenbase:eigenbase-properties",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "net.hydromatic:linq4j",
        "net.hydromatic:quidem"))

    case object v2_0 extends HiveVersion("2.0.1",
      exclusions = Seq("org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v2_1 extends HiveVersion("2.1.1",
      exclusions = Seq("org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    val allSupportedHiveVersions = Set(v12, v13, v14, v1_0, v1_1, v1_2, v2_0, v2_1)
  }
  // scalastyle:on

}
