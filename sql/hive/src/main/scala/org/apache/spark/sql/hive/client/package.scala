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
      val exclusions: Seq[String] = Nil) extends Ordered[HiveVersion] {
    override def compare(that: HiveVersion): Int = {
      val thisVersionParts = fullVersion.split('.').map(_.toInt)
      val thatVersionParts = that.fullVersion.split('.').map(_.toInt)
      assert(thisVersionParts.length == thatVersionParts.length)
      thisVersionParts.zip(thatVersionParts).foreach { case (l, r) =>
        val candidate = l - r
        if (candidate != 0) {
          return candidate
        }
      }
      0
    }
  }

  // scalastyle:off
  private[hive] object hive {
    case object v2_0 extends HiveVersion("2.0.1",
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v2_1 extends HiveVersion("2.1.1",
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v2_2 extends HiveVersion("2.2.0",
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    // Since HIVE-23980, calcite-core included in Hive package jar.
    case object v2_3 extends HiveVersion("2.3.9",
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "com.fasterxml.jackson.core:*",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "org.apache.hive:hive-vector-code-gen"))

    // Since Hive 3.0, HookUtils uses org.apache.logging.log4j.util.Strings
    // Since HIVE-14496, Hive.java uses calcite-core
    case object v3_0 extends HiveVersion("3.0.0",
      extraDeps = Seq("org.apache.logging.log4j:log4j-api:2.10.0",
        "org.apache.derby:derby:10.14.1.0"),
      exclusions = Seq("org.apache.calcite:calcite-druid",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "org.apache.hive:hive-vector-code-gen"))

    // Since Hive 3.0, HookUtils uses org.apache.logging.log4j.util.Strings
    // Since HIVE-14496, Hive.java uses calcite-core
    case object v3_1 extends HiveVersion("3.1.3",
      extraDeps = Seq("org.apache.logging.log4j:log4j-api:2.10.0",
        "org.apache.derby:derby:10.14.1.0"),
      exclusions = Seq("org.apache.calcite:calcite-druid",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "org.apache.hive:hive-vector-code-gen"))

    val allSupportedHiveVersions: Set[HiveVersion] =
      Set(v2_0, v2_1, v2_2, v2_3, v3_0, v3_1)
  }
  // scalastyle:on

}
