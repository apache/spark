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

description = "Spark Project Common Utils Java"

// This module is pure Java
plugins {
    `java-library`
    `maven-publish`
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(SparkVersions.java))
    }
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    // Spark internal modules
    implementation(project(":common:tags"))

    // Logging
    implementation("org.slf4j:slf4j-api:${SparkVersions.slf4j}")
    implementation("org.slf4j:jul-to-slf4j:${SparkVersions.slf4j}")
    implementation("org.slf4j:jcl-over-slf4j:${SparkVersions.slf4j}")
    implementation("org.apache.logging.log4j:log4j-slf4j2-impl:${SparkVersions.log4j}")
    implementation("org.apache.logging.log4j:log4j-api:${SparkVersions.log4j}")
    implementation("org.apache.logging.log4j:log4j-core:${SparkVersions.log4j}")
    implementation("org.apache.logging.log4j:log4j-1.2-api:${SparkVersions.log4j}")
    implementation("org.apache.logging.log4j:log4j-layout-template-json:${SparkVersions.log4j}")

    // JSON
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:${SparkVersions.junit}")
    testImplementation("org.mockito:mockito-core:${SparkVersions.mockito}")
}

// Create test fixtures jar for other modules to use
configurations {
    create("testFixtures")
}

tasks.register<Jar>("testFixturesJar") {
    archiveClassifier.set("test-fixtures")
    from(sourceSets.test.get().output)
}

artifacts {
    add("testFixtures", tasks.named("testFixturesJar"))
}