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

// MLlib Local module dependencies

dependencies {
    // Core Spark dependencies
    implementation(project(":common:tags"))
    implementation(project(":common:utils"))

    // Scala runtime
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")
    implementation("org.scala-lang:scala-reflect:${SparkVersions.scala}")

    // Breeze for numerical computing
    implementation("org.scalanlp:breeze_${SparkVersions.scalaBinary}:2.1.0")

    // Netlib BLAS for linear algebra operations
    implementation("com.github.fommil.netlib:all:1.1.2")
    implementation("com.github.fommil.netlib:core:1.1.2")

    // Apache Commons Math
    implementation("org.apache.commons:commons-math3:${SparkVersions.commonsMath3}")

    // Jackson for JSON serialization
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_${SparkVersions.scalaBinary}:${SparkVersions.jackson}")

    // Test dependencies
    testImplementation("org.scalatest:scalatest_2.13:3.2.17")
    testImplementation("junit:junit:4.13.2")
}
