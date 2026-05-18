# Spark Development Build and Management Tasks

set shell := ["bash", "-c"]

# Default recipe - show help
default:
    @just --list

# ============================================================================
# Environment Management
# ============================================================================

# Load environment variables from .env file
load-env:
    #!/bin/bash
    if [ -f .env ]; then
        set -a
        source .env
        set +a
        echo "Environment loaded from .env"
        env | grep -E "^(SPARK_|JAVA_|MAVEN_|HADOOP_)" | head -20 || true
    else
        echo "No .env file found in current directory"
    fi

# ============================================================================
# Java Version Management
# ============================================================================

# Switch to Java 8
java8:
    #!/bin/bash
    export SDKMAN_DIR="/usr/local/sdkman"
    [[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "${SDKMAN_DIR}/bin/sdkman-init.sh"
    sdk default java 8.0.402-amzn
    java -version

# Switch to Java 11
java11:
    #!/bin/bash
    export SDKMAN_DIR="/usr/local/sdkman"
    [[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "${SDKMAN_DIR}/bin/sdkman-init.sh"
    sdk default java 11.0.22-amzn
    java -version

# Switch to Java 17
java17:
    #!/bin/bash
    export SDKMAN_DIR="/usr/local/sdkman"
    [[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "${SDKMAN_DIR}/bin/sdkman-init.sh"
    sdk default java 17.0.10-amzn
    java -version

# List installed Java versions
java-list:
    #!/bin/bash
    export SDKMAN_DIR="/usr/local/sdkman"
    [[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "${SDKMAN_DIR}/bin/sdkman-init.sh"
    sdk list java | head -30

# ============================================================================
# Maven Version Management
# ============================================================================

# Switch to Maven 3.8.x
maven38:
    #!/bin/bash
    export SDKMAN_DIR="/usr/local/sdkman"
    [[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "${SDKMAN_DIR}/bin/sdkman-init.sh"
    sdk default maven 3.8.8
    mvn --version

# Switch to Maven 3.9.x (recommended for Spark 3.5)
maven39:
    #!/bin/bash
    export SDKMAN_DIR="/usr/local/sdkman"
    [[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "${SDKMAN_DIR}/bin/sdkman-init.sh"
    sdk default maven 3.9.6
    mvn --version

# List installed Maven versions
maven-list:
    #!/bin/bash
    export SDKMAN_DIR="/usr/local/sdkman"
    [[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "${SDKMAN_DIR}/bin/sdkman-init.sh"
    sdk list maven | head -30

# ============================================================================
# Build Tasks
# ============================================================================

# Clean the entire project
clean:
    mvn clean

# Build entire project without tests (fast build)
build-without-tests:
    echo "=== Building Spark without tests ==="
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true

# Build with tests (slower, comprehensive)
build-with-tests:
    echo "=== Building Spark with tests ==="
    mvn clean install -Dmaven.javadoc.skip=true

# Build distribution package (tgz)
build-dist:
    #!/bin/bash
    echo "=== Building Spark distribution ==="
    ./dev/make-distribution.sh --name custom --pip --tgz -Phadoop-3 -Phive -Phive-thriftserver -Pyarn -Pkubernetes

# Build distribution without R support
build-dist-no-r:
    #!/bin/bash
    echo "=== Building Spark distribution (no R) ==="
    ./dev/make-distribution.sh --name custom --pip --tgz -Phadoop-3 -Phive -Phive-thriftserver -Pyarn -Pkubernetes

# Build with specific Hadoop version
build-hadoop version="3.3.4":
    echo "=== Building Spark with Hadoop {{version}} ==="
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Dhadoop.version={{version}}

# Build with YARN support
build-yarn:
    echo "=== Building Spark with YARN ==="
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Pyarn

# Build with Kubernetes support
build-kubernetes:
    echo "=== Building Spark with Kubernetes ==="
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Pkubernetes

# Build with Hive support
build-hive:
    echo "=== Building Spark with Hive ==="
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Phive -Phive-thriftserver

# Build with all common profiles
build-full:
    echo "=== Building Spark with all profiles ==="
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Pyarn -Pkubernetes -Phive -Phive-thriftserver -Phadoop-cloud

# ============================================================================
# Module-specific Build Tasks
# ============================================================================

# Build a specific module (e.g., just build-module sql/core)
build-module module:
    echo "=== Building module: {{module}} ==="
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl {{module}} -am

# Build core module
build-core:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl core -am

# Build SQL core module
build-sql-core:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl sql/core -am

# Build SQL catalyst module
build-catalyst:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl sql/catalyst -am

# Build SQL Hive module
build-sql-hive:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl sql/hive -am

# Build MLlib module
build-mllib:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl mllib -am

# Build Streaming module
build-streaming:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl streaming -am

# Build GraphX module
build-graphx:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl graphx -am

# Build Spark Connect server
build-connect-server:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl connector/connect/server -am

# Build Spark Connect client
build-connect-client:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl connector/connect/client/jvm -am

# Build Kafka connector
build-kafka:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl connector/kafka-0-10-sql -am

# Build Avro connector
build-avro:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl connector/avro -am

# Build assembly module
build-assembly:
    mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl assembly -am

# ============================================================================
# Testing Tasks
# ============================================================================

# Run tests for a specific module
test-module module:
    echo "=== Testing module: {{module}} ==="
    mvn test -pl {{module}}

# Run a specific test class
test-class module class:
    echo "=== Running test class: {{class}} in {{module}} ==="
    mvn test -pl {{module}} -Dtest={{class}}

# Run Scala tests only
test-scala module:
    mvn test -pl {{module}} -Dtest=none -DwildcardSuites={{module}}

# Run tests with specific tags
test-tagged tag:
    mvn test -Dtest.include.tags={{tag}}

# ============================================================================
# Code Quality Tasks
# ============================================================================

# Run checkstyle validation
checkstyle:
    mvn checkstyle:check

# Run scalastyle validation
scalastyle:
    mvn scalastyle:check

# Run both checkstyle and scalastyle
lint: checkstyle scalastyle

# Validate project structure
validate:
    mvn validate

# ============================================================================
# Dependency Tasks
# ============================================================================

# Fetch dependencies only (no build)
fetch-deps:
    mvn dependency:go-offline -DskipTests

# Display dependency tree for a module
deps-tree module="core":
    mvn dependency:tree -pl {{module}}

# Check for dependency updates
deps-updates:
    mvn versions:display-dependency-updates

# ============================================================================
# Utility Tasks
# ============================================================================

# Show current Java version
show-java:
    java -version

# Show current Maven version
show-maven:
    mvn --version

# Show SDKMAN environment
show-sdkman:
    #!/bin/bash
    echo "SDKMAN_DIR: ${SDKMAN_DIR:-not set}"
    echo "JAVA_HOME: ${JAVA_HOME:-not set}"
    echo "M2_HOME: ${M2_HOME:-not set}"
    echo ""
    echo "Current Java:"
    java -version
    echo ""
    echo "Current Maven:"
    mvn --version

# Clear local Maven cache (use with caution)
clear-m2-cache:
    rm -rf ~/.m2/repository/*
    echo "Maven cache cleared"

# Display build information
info:
    #!/bin/bash
    echo "=== Spark Build Environment ==="
    echo ""
    echo "Spark Version: 3.5.5"
    echo "Scala Version: 2.12.18"
    echo ""
    echo "Java Version:"
    java -version 2>&1 | head -1
    echo ""
    echo "Maven Version:"
    mvn --version 2>&1 | head -1
    echo ""
    echo "Workspace: $(pwd)"
    echo ""
    echo "Available profiles:"
    echo "  - hadoop-3, hadoop-provided"
    echo "  - yarn"
    echo "  - kubernetes"
    echo "  - hive, hive-thriftserver, hive-provided"
    echo "  - scala-2.12, scala-2.13"
    echo "  - hadoop-cloud"
    echo "  - kinesis-asl"
    echo ""

# Show this help
help: info
    @just --list

# ============================================================================
# Scala Version Management
# ============================================================================

# Switch to Scala 2.12 (default)
scala212:
    ./dev/change-scala-version.sh 2.12
    echo "Switched to Scala 2.12"

# Switch to Scala 2.13
scala213:
    ./dev/change-scala-version.sh 2.13
    echo "Switched to Scala 2.13"

# ============================================================================
# Development Shortcuts
# ============================================================================

# Quick rebuild after code changes (incremental)
rebuild module="core":
    mvn install -DskipTests -Dmaven.javadoc.skip=true -pl {{module}} -am -o

# Package without running tests
package:
    mvn package -DskipTests -Dmaven.javadoc.skip=true

# Generate IDE project files for IntelliJ
idea:
    mvn idea:idea

# Generate IDE project files for Eclipse
eclipse:
    mvn eclipse:eclipse
