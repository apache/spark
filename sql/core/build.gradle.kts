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
import org.gradle.api.tasks.compile.JavaCompile

plugins {
    scala
    `java-library`
}

description = "Spark Project SQL Core"

dependencies {
    // Core Spark dependencies
    implementation(project(":core"))
    implementation(project(":sql:catalyst"))
    implementation(project(":common:sketch"))
    implementation(project(":common:tags"))
    implementation(project(":common:utils"))
    implementation(project(":common:unsafe"))

    // Scala runtime
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")
    implementation("org.scala-lang:scala-reflect:${SparkVersions.scala}")
    implementation("org.scala-lang.modules:scala-parallel-collections_${SparkVersions.scalaBinary}:${SparkVersions.scalaParallelCollections}")

    // RocksDB for state store
    implementation("org.rocksdb:rocksdbjni:${SparkVersions.rocksdb}")

    // Univocity parsers for CSV processing
    implementation("com.univocity:univocity-parsers:${SparkVersions.univocityParsers}")

    // Parquet format support
    implementation("org.apache.parquet:parquet-hadoop:${SparkVersions.parquet}")
    implementation("org.apache.parquet:parquet-column:${SparkVersions.parquet}")
    implementation("org.apache.parquet:parquet-common:${SparkVersions.parquet}")

    // Apache Avro support
    implementation("org.apache.avro:avro:${SparkVersions.avro}")
    implementation("org.apache.avro:avro-mapred:${SparkVersions.avro}")

    // ORC format support
    implementation("org.apache.orc:orc-core:${SparkVersions.orc}")
    implementation("org.apache.orc:orc-mapreduce:${SparkVersions.orc}")
    implementation("org.apache.orc:orc-format:${SparkVersions.orc}")

    // Hive storage API
    implementation("org.apache.hive:hive-storage-api:${SparkVersions.hive}")

    // Hadoop client libraries
    implementation("org.apache.hadoop:hadoop-client-api:${SparkVersions.hadoop}")
    implementation("org.apache.hadoop:hadoop-client-runtime:${SparkVersions.hadoop}")

    // Test dependencies
    testImplementation(project(":core", "tests"))
    testImplementation(project(":sql:catalyst", "tests"))
    testImplementation(project(":sql:api", "tests"))
    testImplementation(project(":common:tags", "tests"))
    testImplementation("org.scalatest:scalatest_${SparkVersions.scalaBinary}:${SparkVersions.scalatest}")
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.apache.parquet:parquet-encoding:${SparkVersions.parquet}:tests")
    testImplementation("org.apache.parquet:parquet-common:${SparkVersions.parquet}:tests")
    testImplementation("org.apache.parquet:parquet-column:${SparkVersions.parquet}:tests")
}

// Configure mixed Scala/Java compilation
tasks.withType<ScalaCompile>().configureEach {
    // Include Java sources in Scala compilation so Java can reference Scala classes
    source(sourceSets["main"].java.srcDirs)
    source(sourceSets["main"].scala.srcDirs)

    // Suppress deprecation warnings during Maven to Gradle migration
    scalaCompileOptions.additionalParameters = listOf(
        "-Wconf:cat=deprecation:w"
    )
}

// Prevent Java compilation task from running separately since Scala compiler handles both
tasks.named<JavaCompile>("compileJava") {
    enabled = false
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("spark-sql_${SparkVersions.scalaBinary}")
}

// Create a test jar similar to Maven's test-jar goal
val testJar by tasks.registering(Jar::class) {
    archiveBaseName.set("spark-sql_${SparkVersions.scalaBinary}")
    archiveClassifier.set("tests")
    from(sourceSets.test.get().output)
}

configurations {
    create("testJar") {
        extendsFrom(configurations.testImplementation.get())
    }
}

artifacts {
    add("testJar", testJar)
}
