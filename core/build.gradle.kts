import org.gradle.api.tasks.scala.ScalaCompile

plugins {
    scala
    `java-library`
    id("com.google.protobuf") version "0.9.4"
}

description = "Spark Project Core"

dependencies {
    // Scala runtime
    implementation("org.scala-lang:scala-library:${SparkVersions.scala}")
    implementation("org.scala-lang:scala-reflect:${SparkVersions.scala}")
    implementation("org.scala-lang.modules:scala-parallel-collections_${SparkVersions.scalaBinary}:${SparkVersions.scalaParallelCollections}")
    implementation("org.scala-lang.modules:scala-xml_${SparkVersions.scalaBinary}:${SparkVersions.scalaXml}")

    // Core Spark modules
    implementation(project(":launcher"))
    implementation(project(":common:kvstore"))
    implementation(project(":common:network-common"))
    implementation(project(":common:network-shuffle"))
    implementation(project(":common:unsafe"))
    implementation(project(":common:utils"))
    implementation(project(":common:utils-java"))
    implementation(project(":common:variant"))
    implementation(project(":common:tags"))

    // Essential external dependencies
    implementation("com.google.guava:guava:${SparkVersions.guava}")
    implementation("org.apache.hadoop:hadoop-client-api:${SparkVersions.hadoop}")
    implementation("org.apache.hadoop:hadoop-client-runtime:${SparkVersions.hadoop}")

    // Jackson JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind:${SparkVersions.jackson}")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_${SparkVersions.scalaBinary}:${SparkVersions.jackson}")

    // Apache Avro
    implementation("org.apache.avro:avro:${SparkVersions.avro}")

    // Serialization
    implementation("com.twitter:chill_${SparkVersions.scalaBinary}:${SparkVersions.chill}")
    implementation("com.esotericsoftware:kryo-shaded:${SparkVersions.kryo}")

    // Netty
    implementation("io.netty:netty-all:${SparkVersions.netty}")

    // Metrics
    implementation("io.dropwizard.metrics:metrics-core:${SparkVersions.codahaleMetrics}")

    // JSON processing
    implementation("org.json4s:json4s-jackson_${SparkVersions.scalaBinary}:${SparkVersions.json4s}")

    // RoaringBitmap
    implementation("org.roaringbitmap:RoaringBitmap:${SparkVersions.roaringBitmap}")

    // Jetty web server
    implementation("org.eclipse.jetty:jetty-server:${SparkVersions.jetty}")
    implementation("org.eclipse.jetty:jetty-servlet:${SparkVersions.jetty}")

    // Servlet APIs
    implementation("jakarta.servlet:jakarta.servlet-api:${SparkVersions.jakartaServlet}")

    // Logging
    implementation("org.apache.logging.log4j:log4j-core:${SparkVersions.log4j}")
    implementation("org.slf4j:slf4j-api:${SparkVersions.slf4j}")

    // Commons crypto
    implementation("org.apache.commons:commons-crypto:${SparkVersions.commonsCrypto}")

    // JAX-RS for web services
    implementation("jakarta.ws.rs:jakarta.ws.rs-api:3.0.0")

    // Python integration
    implementation("net.sf.py4j:py4j:${SparkVersions.py4j}")

    // Python serialization
    implementation("net.razorvine:pickle:${SparkVersions.pickle}")

    // Apache Commons Collections
    implementation("org.apache.commons:commons-collections4:${SparkVersions.commonsCollections4}")

    // Apache Curator for ZooKeeper
    implementation("org.apache.curator:curator-framework:${SparkVersions.curator}")
    implementation("org.apache.curator:curator-client:${SparkVersions.curator}")
    implementation("org.apache.curator:curator-recipes:${SparkVersions.curator}")
    implementation("org.apache.zookeeper:zookeeper:${SparkVersions.zookeeper}")

    // RocksDB
    implementation("org.rocksdb:rocksdbjni:${SparkVersions.rocksdb}")

    // Compression libraries
    implementation("com.github.luben:zstd-jni:${SparkVersions.zstd}")
    implementation("com.ning:compress-lzf:${SparkVersions.compressLzf}")

    // Additional Dropwizard metrics components
    implementation("io.dropwizard.metrics:metrics-graphite:${SparkVersions.codahaleMetrics}")
    implementation("io.dropwizard.metrics:metrics-jmx:${SparkVersions.codahaleMetrics}")
    implementation("io.dropwizard.metrics:metrics-json:${SparkVersions.codahaleMetrics}")
    implementation("io.dropwizard.metrics:metrics-jvm:${SparkVersions.codahaleMetrics}")

    // ASM library and XBean
    implementation("org.ow2.asm:asm:${SparkVersions.asm}")
    implementation("org.apache.xbean:xbean-asm9-shaded:${SparkVersions.xbean}")

    // LZ4 compression
    implementation("org.lz4:lz4-java:${SparkVersions.lz4}")

    // Apache Commons Math3
    implementation("org.apache.commons:commons-math3:${SparkVersions.commonsMath3}")

    // Clearspring Analytics (HyperLogLog)
    implementation("com.clearspring.analytics:stream:${SparkVersions.clearspringAnalytics}")

    // FuseSource LevelDB JNI
    implementation("org.fusesource.leveldbjni:leveldbjni-all:1.8")

    // Jersey for REST APIs
    implementation("org.glassfish.jersey.core:jersey-server:${SparkVersions.jersey}")
    implementation("org.glassfish.jersey.containers:jersey-container-servlet:${SparkVersions.jersey}")
    implementation("org.glassfish.jersey.inject:jersey-hk2:${SparkVersions.jersey}")

    // Protocol Buffers
    implementation("com.google.protobuf:protobuf-java:${SparkVersions.protobuf}")

    // Apache Commons Text
    implementation("org.apache.commons:commons-text:${SparkVersions.commonsText}")

    // JWT libraries
    implementation("io.jsonwebtoken:jjwt-api:${SparkVersions.jjwt}")
    implementation("io.jsonwebtoken:jjwt-impl:${SparkVersions.jjwt}")
    implementation("io.jsonwebtoken:jjwt-jackson:${SparkVersions.jjwt}")

    // Jetty Client
    implementation("org.eclipse.jetty:jetty-client:${SparkVersions.jetty}")
    implementation("org.eclipse.jetty:jetty-proxy:${SparkVersions.jetty}")

    // Apache Ivy
    implementation("org.apache.ivy:ivy:${SparkVersions.ivy}")

    // Test dependencies
    testImplementation("org.scalatest:scalatest_2.13:3.2.17")
    testImplementation("junit:junit:4.13.2")
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
tasks.named("compileJava") {
    enabled = false
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("spark-core_2.13")
}

// Configure protobuf
protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${SparkVersions.protobuf}"
    }
    generateProtoTasks {
        all().forEach { task ->
            task.builtins {
                named("java")
            }
        }
    }
}