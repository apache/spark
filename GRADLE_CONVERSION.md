# Apache Spark Maven to Gradle Conversion

## Overview

This document describes the conversion of the Apache Spark project from Maven to Gradle with Kotlin DSL. The conversion maintains the same multi-module structure and functionality while leveraging Gradle's performance benefits and modern build features.

## Conversion Summary

### Files Created/Modified

#### Root Level Files
- `settings.gradle.kts` - Project structure and module definitions
- `build.gradle.kts` - Root project configuration
- `gradlew`, `gradlew.bat` - Gradle wrapper scripts
- `gradle/wrapper/gradle-wrapper.properties` - Wrapper configuration

#### Build Logic (buildSrc/)
- `buildSrc/build.gradle.kts` - Build logic dependencies
- `buildSrc/src/main/kotlin/SparkVersions.kt` - Centralized version management
- `buildSrc/src/main/kotlin/spark-common.gradle.kts` - Shared build configuration

#### Module Build Files
- `core/build.gradle.kts` - Core module (fully configured)
- `launcher/build.gradle.kts` - Launcher module (fully configured)
- `common/utils/build.gradle.kts` - Common utils (fully configured)
- `common/utils-java/build.gradle.kts` - Common utils Java (fully configured)
- `common/tags/build.gradle.kts` - Common tags (fully configured)
- Plus 36+ additional module build.gradle.kts files with basic structure

## Project Structure

The Gradle conversion maintains the exact same module structure as the Maven build:

```
spark/
├── common/
│   ├── sketch/
│   ├── kvstore/
│   ├── network-common/
│   ├── network-shuffle/
│   ├── unsafe/
│   ├── utils/
│   ├── utils-java/
│   ├── variant/
│   ├── tags/
│   └── network-yarn/
├── sql/
│   ├── api/
│   ├── catalyst/
│   ├── core/
│   ├── hive/
│   ├── pipelines/
│   ├── connect/
│   │   ├── shims/
│   │   ├── server/
│   │   ├── common/
│   │   └── client/jvm/
│   └── hive-thriftserver/
├── core/
├── graphx/
├── mllib/
├── mllib-local/
├── streaming/
├── assembly/
├── examples/
├── repl/
├── launcher/
├── tools/
├── hadoop-cloud/
├── resource-managers/
│   ├── yarn/
│   └── kubernetes/
│       ├── core/
│       └── integration-tests/
└── connector/
    ├── kafka-0-10-token-provider/
    ├── kafka-0-10/
    ├── kafka-0-10-assembly/
    ├── kafka-0-10-sql/
    ├── avro/
    ├── protobuf/
    ├── spark-ganglia-lgpl/
    ├── kinesis-asl/
    ├── kinesis-asl-assembly/
    ├── docker-integration-tests/
    └── profiler/
```

## Key Features of the Conversion

### 1. Centralized Version Management
All dependency versions are managed in `buildSrc/src/main/kotlin/SparkVersions.kt`:
- Spark version: 4.1.0-SNAPSHOT
- Java: 17
- Scala: 2.13.16
- All third-party library versions

### 2. Shared Build Logic
The `spark-common.gradle.kts` plugin provides:
- Common Scala compilation settings
- Standard test configuration
- Publishing setup
- Common dependencies (Scala, logging, testing)

### 3. Profile Support
Maven profiles have been converted to Gradle build variants:
- `hadoop-provided` - Excludes Hadoop dependencies
- `hive-provided` - Excludes Hive dependencies
- `derby-provided` - Excludes Derby dependencies

Usage: `./gradlew build -Pprofiles=hadoop-provided,hive-provided`

### 4. Dependency Management
- Force resolution of conflicting dependency versions
- Exclusion of problematic transitive dependencies
- Proper handling of Scala binary versions

## Key Gradle Advantages

### Performance Benefits
- **Incremental Compilation**: Only recompiles changed files
- **Build Cache**: Reuses outputs from previous builds
- **Parallel Execution**: Builds multiple modules simultaneously
- **Configuration Cache**: Faster configuration phase

### Developer Experience
- **Kotlin DSL**: Type-safe, IDE-friendly build scripts
- **Dependency Insights**: Better dependency conflict resolution
- **Flexible Task System**: Easy to add custom build tasks
- **Modern Tooling**: Better IDE integration

## Usage Examples

### Common Commands

```bash
# Build all modules
./gradlew build

# Build specific module
./gradlew :core:build

# Run tests for all modules
./gradlew testAll

# Assemble all modules
./gradlew assembleAll

# Publish all modules
./gradlew publishAll

# View project structure
./gradlew projectStructure

# Build with profiles
./gradlew build -Pprofiles=hadoop-provided
```

### Development Workflow

1. **Clean Build**: `./gradlew clean build`
2. **Incremental Build**: `./gradlew build` (subsequent runs)
3. **Test Single Module**: `./gradlew :core:test`
4. **Continuous Testing**: `./gradlew test --continuous`

## Migration Notes

### What Works
✅ Project structure recognition
✅ Basic compilation (Java modules)
✅ Gradle wrapper setup
✅ Module dependency structure
✅ Version management
✅ Profile system

### What Needs Additional Work
⚠️ **Module-Specific Dependencies**: Each module's build.gradle.kts needs detailed dependency configuration based on its pom.xml
⚠️ **Scala Compilation**: Scala modules need proper source sets and compilation settings
⚠️ **Plugin Conversion**: Maven-specific plugins need Gradle equivalents:
   - Scala compilation plugins
   - Assembly plugins
   - Code generation plugins
   - Test plugins

⚠️ **Resource Processing**: Some modules may have custom resource processing
⚠️ **Integration Tests**: Test configuration may need module-specific tuning

## Next Steps

### Phase 2: Complete Module Configuration
1. **Analyze each pom.xml** for module-specific dependencies
2. **Configure Scala compilation** for Scala modules
3. **Set up assembly plugins** for distribution modules
4. **Configure integration tests** with proper classpaths

### Phase 3: Plugin Migration
1. **Protobuf compilation** - Convert protoc-jar-maven-plugin
2. **Assembly creation** - Convert maven-assembly-plugin
3. **Scala compilation** - Fine-tune scala-maven-plugin equivalents
4. **Code generation** - Convert any code generation plugins

### Phase 4: Optimization
1. **Build caching** configuration
2. **Parallel execution** tuning
3. **Custom tasks** for Spark-specific workflows
4. **CI/CD integration** updates

## Verification Commands

```bash
# Verify project structure
./gradlew projects

# Check dependencies
./gradlew dependencies

# Verify compilation
./gradlew compileJava compileScala

# Run basic tests
./gradlew test -x :core:test

# Generate dependency reports
./gradlew dependencyInsight --dependency scala-library
```

## Troubleshooting

### Common Issues

1. **Missing Dependencies**: Add to appropriate build.gradle.kts
2. **Version Conflicts**: Check `gradle dependencies` and add force resolution
3. **Compilation Errors**: Verify source sets and classpath configuration
4. **Test Failures**: Check test dependencies and system properties

### Getting Help

```bash
# Gradle help
./gradlew help

# Task details
./gradlew help --task build

# Dependency insight
./gradlew dependencyInsight --dependency [artifact-name]

# Debug information
./gradlew build --info --debug
```

## Performance Comparison

The Gradle build is expected to provide:
- **2-10x faster incremental builds** due to up-to-date checking
- **Parallel module compilation** reducing wall-clock time
- **Build caching** for repeated builds
- **Configuration caching** for faster startup

## Conclusion

This conversion provides a solid foundation for migrating Apache Spark from Maven to Gradle. The core structure, version management, and build logic are in place. The next phase involves completing module-specific configurations and testing the full build pipeline.

The conversion maintains full compatibility with the existing project structure while providing the performance and developer experience benefits of modern Gradle builds.