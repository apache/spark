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

description = "Spark Project Networking"

dependencies {
    // Spark internal modules
    implementation(project(":common:utils-java"))
    implementation(project(":common:tags"))

    // Netty dependencies
    implementation("io.netty:netty-all:${SparkVersions.netty}")
    implementation("io.netty:netty-transport-native-epoll:${SparkVersions.netty}") {
        artifact {
            classifier = "linux-x86_64"
        }
    }
    implementation("io.netty:netty-transport-native-epoll:${SparkVersions.netty}") {
        artifact {
            classifier = "linux-aarch_64"
        }
    }
    implementation("io.netty:netty-transport-native-kqueue:${SparkVersions.netty}") {
        artifact {
            classifier = "osx-aarch_64"
        }
    }
    implementation("io.netty:netty-transport-native-kqueue:${SparkVersions.netty}") {
        artifact {
            classifier = "osx-x86_64"
        }
    }
    implementation("io.netty:netty-tcnative-boringssl-static:${SparkVersions.nettyTcnative}") {
        artifact {
            classifier = "linux-x86_64"
        }
    }
    implementation("io.netty:netty-tcnative-boringssl-static:${SparkVersions.nettyTcnative}") {
        artifact {
            classifier = "linux-aarch_64"
        }
    }
    implementation("io.netty:netty-tcnative-boringssl-static:${SparkVersions.nettyTcnative}") {
        artifact {
            classifier = "osx-aarch_64"
        }
    }
    implementation("io.netty:netty-tcnative-boringssl-static:${SparkVersions.nettyTcnative}") {
        artifact {
            classifier = "osx-x86_64"
        }
    }

    // LevelDB and RocksDB
    implementation("org.fusesource.leveldbjni:leveldbjni-all:1.8")
    implementation("org.rocksdb:rocksdbjni:9.8.4")

    // Jackson for JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.core:jackson-annotations:${SparkVersions.jackson}")

    // Metrics
    implementation("io.dropwizard.metrics:metrics-core:${SparkVersions.codahaleMetrics}")

    // Provided dependencies
    implementation("org.slf4j:slf4j-api:${SparkVersions.slf4j}") // Note: was 'provided' in Maven
    implementation("com.google.code.findbugs:jsr305")
    implementation("com.google.guava:guava:33.4.0-jre")
    implementation("org.apache.commons:commons-crypto:${SparkVersions.commonsCrypto}")
    implementation("com.google.crypto.tink:tink:${SparkVersions.tink}")
    implementation("org.roaringbitmap:RoaringBitmap:${SparkVersions.roaringBitmap}")

    // Test dependencies
    testImplementation("org.apache.logging.log4j:log4j-api:${SparkVersions.log4j}")
    testImplementation("org.apache.logging.log4j:log4j-core:${SparkVersions.log4j}")
    testImplementation("org.apache.logging.log4j:log4j-1.2-api:${SparkVersions.log4j}")
    testImplementation("org.apache.logging.log4j:log4j-slf4j2-impl:${SparkVersions.log4j}")
    testImplementation("org.bouncycastle:bcprov-jdk18on")
    testImplementation("org.bouncycastle:bcpkix-jdk18on")
    testImplementation("org.mockito:mockito-core:${SparkVersions.mockito}")
    testImplementation("net.bytebuddy:byte-buddy")
    testImplementation("net.bytebuddy:byte-buddy-agent")

    // Test-jar dependency for tags
    testImplementation(project(":common:tags")) {
        attributes {
            attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category::class, Category.VERIFICATION))
            attribute(TestSuiteType.TEST_SUITE_TYPE_ATTRIBUTE, objects.named(TestSuiteType::class, TestSuiteType.UNIT_TEST))
        }
    }
}
