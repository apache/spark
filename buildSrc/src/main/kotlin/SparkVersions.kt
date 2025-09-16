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

/**
 * Centralized version management for Spark build
 */
object SparkVersions {
    const val spark = "4.1.0-SNAPSHOT"

    // Language versions
    const val java = "17"
    const val javaMinimum = "17.0.11"
    const val scala = "2.13.16"
    const val scalaBinary = "2.13"

    // Maven versions
    const val maven = "3.9.11"
    const val execMavenPlugin = "3.5.1"

    // Core dependencies
    const val asm = "9.8"
    const val slf4j = "2.0.17"
    const val log4j = "2.25.1"
    const val hadoop = "3.4.2"
    const val protobuf = "4.29.3"
    const val protocJarMavenPlugin = "3.11.4"
    const val zookeeper = "3.9.3"
    const val curator = "5.9.0"
    const val hive = "2.3.10"
    const val kafka = "3.9.1"
    const val derby = "10.16.1.1"
    const val parquet = "1.16.0"
    const val orc = "2.2.0"
    const val jetty = "11.0.26"
    const val jakartaServlet = "5.0.0"
    const val javaxServlet = "4.0.1"
    const val chill = "0.10.0"
    const val kryo = "4.0.3"
    const val ivy = "2.5.3"
    const val oro = "2.0.8"
    const val xbean = "4.27"
    const val codahaleMetrics = "4.2.33"
    const val avro = "1.12.0"
    const val jackson = "2.19.2"
    const val awsKinesisClient = "1.12.0"
    const val awsJavaSdk = "1.11.655"
    const val awsJavaSdkV2 = "2.29.52"
    const val awsKinesisProducer = "0.12.8"
    const val commonsCollections = "3.2.2"
    const val commonsCollections4 = "4.5.0"
    const val commonsIo = "2.18.0"
    const val commonsLang3 = "3.18.0"
    const val commonsText = "1.14.0"
    const val scalatestMavenPlugin = "2.2.0"

    // Additional dependencies for unsafe module
    const val icu4j = "77.1"
    const val jsr305 = "3.0.0"
    const val objenesis = "3.3"
    const val guava = "33.4.0-jre"
    const val byteBuddy = "1.17.6"

    // Network and crypto dependencies
    const val netty = "4.1.126.Final"
    const val nettyTcnative = "2.0.72.Final"
    const val commonsCrypto = "1.1.0"
    const val tink = "1.16.0"
    const val roaringBitmap = "1.3.0"
    const val rocksdb = "8.11.4"

    // Compression dependencies
    const val snappy = "1.1.10.8"
    const val lz4 = "1.8.0"
    const val zstd = "1.5.6-9"
    const val compressLzf = "1.1.2"

    // Jersey dependencies
    const val jersey = "3.0.18"

    // Misc Apache Commons
    const val commonsCodec = "1.19.0"
    const val commonsCompress = "1.28.0"
    const val commonsMath3 = "3.6.1"
    const val commonsLang2 = "2.6"

    // Data processing
    const val clearspringAnalytics = "2.9.8"
    const val json4s = "4.0.7"
    const val scalaXml = "2.1.0"
    const val scalaParallelCollections = "1.0.4"

    // JWT dependencies
    const val jjwt = "0.12.6"

    // Python integration
    const val py4j = "0.10.9.9"
    const val pickle = "1.5"

    // Additional dependencies for catalyst module
    const val janino = "3.1.9"
    const val univocityParsers = "2.9.1"
    const val xmlschemaCore = "2.3.1"
    const val datasketches = "6.2.0"
    const val scalaParserCombinators = "2.4.0"
    const val arrow = "18.3.0"
    const val antlr4 = "4.9.3"

    // Test dependencies
    const val junit = "5.13.1"
    const val scalatest = "3.2.17"
    const val mockito = "5.8.0"
    const val jnrPosix = "3.1.16"
    const val selenium = "4.21.0"
    const val htmlunitDriver = "4.21.0"
    const val httpClient = "4.5.14"
    const val httpCore = "4.4.16"
}