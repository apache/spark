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

// Examples module dependencies

dependencies {
    // Core Spark dependencies
    implementation(project(":core"))
    implementation(project(":streaming"))
    implementation(project(":mllib"))
    implementation(project(":mllib-local"))
    implementation(project(":graphx"))
    implementation(project(":sql:core"))
    implementation(project(":sql:catalyst"))

    // Scala runtime
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")

    // Test dependencies are minimal for examples
    testImplementation("org.scalatest:scalatest_2.13:3.2.17")
    testImplementation("junit:junit:4.13.2")
}
