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

import org.gradle.api.tasks.scala.ScalaCompile

description = "Spark Project Common Utils"

dependencies {
    // Other Spark modules
    implementation(project(":common:tags"))
    implementation(project(":common:utils-java"))

    // Test fixtures from utils-java
    testImplementation(project(":common:utils-java")) {
        attributes {
            attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category::class, Category.VERIFICATION))
            attribute(TestSuiteType.TEST_SUITE_TYPE_ATTRIBUTE, objects.named(TestSuiteType::class, TestSuiteType.UNIT_TEST))
        }
    }

    // ASM
    implementation("org.apache.xbean:xbean-asm9-shaded:${SparkVersions.xbean}")

    // JSON
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_${SparkVersions.scalaBinary}:${SparkVersions.jackson}")

    // Ivy
    implementation("org.apache.ivy:ivy:${SparkVersions.ivy}")
    implementation("oro:oro:${SparkVersions.oro}")

    // Logging
    implementation("org.slf4j:slf4j-api:${SparkVersions.slf4j}")
    implementation("org.slf4j:jul-to-slf4j:${SparkVersions.slf4j}")
    implementation("org.slf4j:jcl-over-slf4j:${SparkVersions.slf4j}")
    implementation("org.apache.logging.log4j:log4j-slf4j2-impl:${SparkVersions.log4j}")
    implementation("org.apache.logging.log4j:log4j-api:${SparkVersions.log4j}")
    implementation("org.apache.logging.log4j:log4j-core:${SparkVersions.log4j}")
    implementation("org.apache.logging.log4j:log4j-1.2-api:${SparkVersions.log4j}")
    implementation("org.apache.logging.log4j:log4j-layout-template-json:${SparkVersions.log4j}")

    // Test dependencies
    testImplementation("org.apache.hadoop:hadoop-minikdc:${SparkVersions.hadoop}")
    testImplementation("org.apache.kerby:kerb-simplekdc:2.0.3")
}

// Configure mixed Scala/Java compilation
tasks.withType<ScalaCompile>().configureEach {
    // Include Java sources in Scala compilation so Java can reference Scala classes
    source(sourceSets["main"].java.srcDirs)
    source(sourceSets["main"].scala.srcDirs)
}

// Prevent Java compilation task from running separately since Scala compiler handles both
tasks.named("compileJava") {
    enabled = false
}

// Also configure test compilation for mixed Scala/Java sources
tasks.withType<ScalaCompile>().matching { it.name.contains("Test") }.configureEach {
    source(sourceSets["test"].java.srcDirs)
    source(sourceSets["test"].scala.srcDirs)
}

tasks.named("compileTestJava") {
    enabled = false
}