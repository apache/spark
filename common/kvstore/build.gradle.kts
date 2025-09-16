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

description = "Spark Project Local DB"

dependencies {
    // Spark internal modules
    implementation(project(":common:tags"))
    implementation(project(":common:utils"))
    implementation(project(":common:utils-java"))

    // Guava
    implementation("com.google.guava:guava:33.4.0-jre")

    // LevelDB
    implementation("org.fusesource.leveldbjni:leveldbjni-all:1.8")

    // Jackson for JSON processing
    implementation("com.fasterxml.jackson.core:jackson-core:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-annotations:${SparkVersions.jackson}")

    // Logging
    implementation("org.slf4j:slf4j-api:${SparkVersions.slf4j}")

    // RocksDB
    implementation("org.rocksdb:rocksdbjni:9.8.4")

    // Test dependencies
    testImplementation("org.apache.logging.log4j:log4j-api:${SparkVersions.log4j}")
    testImplementation("org.apache.logging.log4j:log4j-core:${SparkVersions.log4j}")
    testImplementation("org.apache.logging.log4j:log4j-1.2-api:${SparkVersions.log4j}")
    testImplementation("org.apache.logging.log4j:log4j-slf4j2-impl:${SparkVersions.log4j}")
    testImplementation("io.dropwizard.metrics:metrics-core:${SparkVersions.codahaleMetrics}")

    // Test-jar dependency for tags
    testImplementation(project(":common:tags")) {
        attributes {
            attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category::class, Category.VERIFICATION))
            attribute(TestSuiteType.TEST_SUITE_TYPE_ATTRIBUTE, objects.named(TestSuiteType::class, TestSuiteType.UNIT_TEST))
        }
    }
}
