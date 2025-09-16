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

// MLlib module dependencies

dependencies {
    // Core Spark dependencies
    implementation(project(":core"))
    implementation(project(":streaming"))
    implementation(project(":sql:catalyst"))
    implementation(project(":sql:api"))        // For Dataset, DataFrame types
    implementation(project(":mllib-local"))
    implementation(project(":common:tags"))
    implementation(project(":common:utils"))
    implementation(project(":common:utils-java"))   // For LogKeys
    implementation(project(":common:unsafe"))        // For unsafe types

    // Scala runtime
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")
    implementation("org.scala-lang:scala-reflect:${SparkVersions.scala}")

    // Breeze for numerical computing
    implementation("org.scalanlp:breeze_${SparkVersions.scalaBinary}:2.1.0")

    // Apache Commons Math
    implementation("org.apache.commons:commons-math3:${SparkVersions.commonsMath3}")

    // JSON processing (needed by ML pipeline serialization)
    implementation("org.json4s:json4s-jackson_${SparkVersions.scalaBinary}:${SparkVersions.json4s}")
    implementation("org.json4s:json4s-core_${SparkVersions.scalaBinary}:${SparkVersions.json4s}")

    // Hadoop for file system operations
    implementation("org.apache.hadoop:hadoop-client-api:${SparkVersions.hadoop}")

    // Python integration
    implementation("net.sf.py4j:py4j:${SparkVersions.py4j}")

    // Scala XML for shared params code generation
    implementation("org.scala-lang.modules:scala-xml_${SparkVersions.scalaBinary}:${SparkVersions.scalaXml}")

    // Test dependencies
    testImplementation(project(":core", "tests"))
    testImplementation("org.scalatest:scalatest_${SparkVersions.scalaBinary}:${SparkVersions.scalatest}")
    testImplementation("junit:junit:4.13.2")
}
