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

// GraphX module dependencies

dependencies {
    // Core Spark dependencies
    implementation(project(":core"))
    implementation(project(":common:tags"))
    implementation(project(":common:utils"))
    implementation(project(":common:utils-java"))  // For LogKeys
    implementation(project(":mllib-local"))        // For BLAS and Vector operations

    // Scala runtime
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")
    implementation("org.scala-lang:scala-reflect:${SparkVersions.scala}")

    // Breeze for numerical computing
    implementation("org.scalanlp:breeze_${SparkVersions.scalaBinary}:2.1.0")

    // Test dependencies
    testImplementation(project(":core", "tests"))
    testImplementation("org.scalatest:scalatest_2.13:3.2.17")
    testImplementation("junit:junit:4.13.2")
}
