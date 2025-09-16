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

// Module-specific dependencies based on pom.xml

dependencies {
    // Core Spark dependencies
    implementation(project(":common:network-shuffle"))
    implementation(project(":common:network-common"))  // Provides ConfigProvider, TransportContext, etc.
    implementation(project(":common:utils-java"))       // Provides SparkLogger, JavaUtils, etc.
    implementation(project(":common:tags"))

    // Hadoop dependencies (provided scope in Maven, but needed for compilation)
    implementation("org.apache.hadoop:hadoop-client-api:${SparkVersions.hadoop}")
    implementation("org.apache.hadoop:hadoop-client-runtime:${SparkVersions.hadoop}")

    // Jackson JSON processing dependencies
    implementation("com.fasterxml.jackson.core:jackson-core:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-annotations:${SparkVersions.jackson}")

    // Google Guava (provides @VisibleForTesting)
    implementation("com.google.guava:guava:${SparkVersions.guava}")

    // Dropwizard Metrics (for Timer, Gauge, Counter, Meter)
    implementation("io.dropwizard.metrics:metrics-core:${SparkVersions.codahaleMetrics}")

    // SLF4J logging
    implementation("org.slf4j:slf4j-api:${SparkVersions.slf4j}")

    // Test dependencies
    testImplementation(project(":common:tags", "tests"))
}
