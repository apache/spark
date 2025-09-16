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

// Module-specific dependencies based on pom.xml and code analysis

dependencies {
    // Common dependencies will be inherited from spark-common plugin

    // Jackson JSON processing dependencies
    implementation("com.fasterxml.jackson.core:jackson-core:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-annotations:${SparkVersions.jackson}")

    // Spark internal dependencies
    implementation(project(":common:utils"))
    implementation(project(":common:utils-java"))

    // Test dependencies
    testImplementation(project(":common:tags", "test"))
}
