# Spark Development Guide

This guide provides information for developers working on Apache Spark.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Building Spark](#building-spark)
- [Testing](#testing)
- [Code Style](#code-style)
- [IDE Setup](#ide-setup)
- [Debugging](#debugging)
- [Working with Git](#working-with-git)
- [Common Development Tasks](#common-development-tasks)

## Getting Started

### Prerequisites

- Java 17 or Java 21 (for Spark 4.x)
- Maven 3.9.9 or later
- Python 3.9+ (for PySpark development)
- R 4.0+ (for SparkR development)
- Git

### Initial Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/apache/spark.git
   cd spark
   ```

2. **Build Spark:**
   ```bash
   ./build/mvn -DskipTests clean package
   ```

3. **Verify the build:**
   ```bash
   ./bin/spark-shell
   ```

## Development Environment

### Directory Structure

```
spark/
├── assembly/          # Final assembly JAR creation
├── bin/              # User command scripts (spark-submit, spark-shell, etc.)
├── build/            # Build scripts and Maven wrapper
├── common/           # Common utilities and modules
├── conf/             # Configuration templates
├── core/             # Spark Core
├── dev/              # Development tools (run-tests, lint, etc.)
├── docs/             # Documentation (Jekyll-based)
├── examples/         # Example programs
├── python/           # PySpark implementation
├── R/                # SparkR implementation
├── sbin/             # Admin scripts (start-all.sh, stop-all.sh, etc.)
├── sql/              # Spark SQL
└── [other modules]
```

### Key Development Directories

- `dev/`: Contains scripts for testing, linting, and releasing
- `dev/run-tests`: Main test runner
- `dev/lint-*`: Various linting tools
- `build/mvn`: Maven wrapper script

## Building Spark

### Full Build

```bash
# Build all modules, skip tests
./build/mvn -DskipTests clean package

# Build with specific Hadoop version
./build/mvn -Phadoop-3.4 -DskipTests clean package

# Build with Hive support
./build/mvn -Phive -Phive-thriftserver -DskipTests package
```

### Module-Specific Builds

```bash
# Build only core module
./build/mvn -pl core -DskipTests package

# Build core and its dependencies
./build/mvn -pl core -am -DskipTests package

# Build SQL module
./build/mvn -pl sql/core -am -DskipTests package
```

### Build Profiles

Common Maven profiles:

- `-Phadoop-3.4`: Build with Hadoop 3.4
- `-Pyarn`: Include YARN support
- `-Pkubernetes`: Include Kubernetes support
- `-Phive`: Include Hive support
- `-Phive-thriftserver`: Include Hive Thrift Server
- `-Pscala-2.13`: Build with Scala 2.13

### Fast Development Builds

For faster iteration during development:

```bash
# Skip Scala and Java style checks
./build/mvn -DskipTests -Dcheckstyle.skip package

# Build specific module quickly
./build/mvn -pl sql/core -am -DskipTests -Dcheckstyle.skip package
```

## Testing

### Running All Tests

```bash
# Run all tests (takes several hours)
./dev/run-tests

# Run tests for specific modules
./dev/run-tests --modules sql
```

### Running Specific Test Suites

#### Scala/Java Tests

```bash
# Run all tests in a module
./build/mvn test -pl core

# Run a specific test suite
./build/mvn test -pl core -Dtest=SparkContextSuite

# Run specific test methods
./build/mvn test -pl core -Dtest=SparkContextSuite#testJobInterruption
```

#### Python Tests

```bash
# Run all PySpark tests
cd python && python run-tests.py

# Run specific test file
cd python && python -m pytest pyspark/tests/test_context.py

# Run specific test method
cd python && python -m pytest pyspark/tests/test_context.py::SparkContextTests::test_stop
```

#### R Tests

```bash
cd R
R CMD check --no-manual --no-build-vignettes spark
```

### Test Coverage

```bash
# Generate coverage report
./build/mvn clean install -DskipTests
./dev/run-tests --coverage
```

## Code Style

### Scala Code Style

Spark uses Scalastyle for Scala code checking:

```bash
# Check Scala style
./dev/lint-scala

# Auto-format (if scalafmt is configured)
./build/mvn scala:format
```

Key style guidelines:
- 2-space indentation
- Max line length: 100 characters
- Follow [Scala style guide](https://docs.scala-lang.org/style/)

### Java Code Style

Java code follows Google Java Style:

```bash
# Check Java style
./dev/lint-java
```

Key guidelines:
- 2-space indentation
- Max line length: 100 characters
- Use Java 17+ features appropriately

### Python Code Style

PySpark follows PEP 8:

```bash
# Check Python style
./dev/lint-python

# Auto-format with black (if available)
cd python && black pyspark/
```

Key guidelines:
- 4-space indentation
- Max line length: 100 characters
- Type hints encouraged for new code

## IDE Setup

### IntelliJ IDEA

1. **Import Project:**
   - File → Open → Select `pom.xml`
   - Choose "Open as Project"
   - Import Maven projects automatically

2. **Configure JDK:**
   - File → Project Structure → Project SDK → Select Java 17 or 21

3. **Recommended Plugins:**
   - Scala plugin
   - Python plugin
   - Maven plugin

4. **Code Style:**
   - Import Spark code style from `dev/scalastyle-config.xml`

### Visual Studio Code

1. **Recommended Extensions:**
   - Scala (Metals)
   - Python
   - Maven for Java

2. **Workspace Settings:**
   ```json
   {
     "java.configuration.maven.userSettings": ".mvn/settings.xml",
     "python.linting.enabled": true,
     "python.linting.pylintEnabled": true
   }
   ```

### Eclipse

1. **Import Project:**
   - File → Import → Maven → Existing Maven Projects

2. **Install Plugins:**
   - Scala IDE
   - Maven Integration

## Debugging

### Debugging Scala/Java Code

#### Using IDE Debugger

1. Run tests with debugging enabled in your IDE
2. Set breakpoints in source code
3. Run test in debug mode

#### Command Line Debugging

```bash
# Enable remote debugging
export SPARK_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
./bin/spark-shell
```

Then attach your IDE debugger to port 5005.

### Debugging PySpark

```bash
# Enable Python debugging
export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python

# Run with pdb
python -m pdb your_spark_script.py
```

### Logging

Adjust log levels in `conf/log4j2.properties`:

```properties
# Set root logger level
rootLogger.level = info

# Set specific logger
logger.spark.name = org.apache.spark
logger.spark.level = debug
```

## Working with Git

### Branch Naming

- Feature branches: `feature/description`
- Bug fixes: `fix/issue-number-description`
- Documentation: `docs/description`

### Commit Messages

Follow conventional commit format:

```
[SPARK-XXXXX] Brief description (max 72 chars)

Detailed description of the change, motivation, and impact.

- Bullet points for specific changes
- Reference related issues

Closes #XXXXX
```

### Creating Pull Requests

1. **Fork the repository** on GitHub
2. **Create a feature branch** from master
3. **Make your changes** with clear commits
4. **Push to your fork**
5. **Open a Pull Request** with:
   - Clear title and description
   - Link to JIRA issue if applicable
   - Unit tests for new functionality
   - Documentation updates if needed

### Code Review

- Address review comments promptly
- Keep discussions professional and constructive
- Be open to suggestions and improvements

## Common Development Tasks

### Adding a New Configuration

1. Define config in appropriate config file (e.g., `sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala`)
2. Document the configuration
3. Add tests
4. Update documentation in `docs/configuration.md`

### Adding a New API

1. Implement the API with proper documentation
2. Add comprehensive unit tests
3. Update relevant documentation
4. Consider backward compatibility
5. Add deprecation notices if replacing old APIs

### Adding a New Data Source

1. Implement `DataSourceV2` interface
2. Add read/write support
3. Include integration tests
4. Document usage in `docs/sql-data-sources-*.md`

### Performance Optimization

1. Identify bottleneck with profiling
2. Create benchmark to measure improvement
3. Implement optimization
4. Verify performance gain
5. Ensure no functionality regression

### Updating Dependencies

1. Check for security vulnerabilities
2. Test compatibility
3. Update version in `pom.xml`
4. Update `LICENSE` and `NOTICE` files if needed
5. Run full test suite

## Useful Commands

```bash
# Clean build artifacts
./build/mvn clean

# Skip Scalastyle checks
./build/mvn -Dscalastyle.skip package

# Generate API documentation
./build/mvn scala:doc

# Check for dependency updates
./build/mvn versions:display-dependency-updates

# Profile a build
./build/mvn clean package -Dprofile

# Run Spark locally with different memory
./bin/spark-shell --driver-memory 4g --executor-memory 4g
```

## Troubleshooting

### Build Issues

- **Out of Memory**: Increase Maven memory with `export MAVEN_OPTS="-Xmx4g"`
- **Compilation errors**: Clean build with `./build/mvn clean`
- **Version conflicts**: Update local Maven repo: `./build/mvn -U package`

### Test Failures

- Run single test to isolate issue
- Check for environment-specific problems
- Review logs in `target/` directories
- Enable debug logging for more detail

### IDE Issues

- Reimport Maven project
- Invalidate caches and restart
- Check SDK and language level settings

## Resources

- [Apache Spark Website](https://spark.apache.org/)
- [Spark Developer Tools](https://spark.apache.org/developer-tools.html)
- [Spark Wiki](https://cwiki.apache.org/confluence/display/SPARK)
- [Spark Mailing Lists](https://spark.apache.org/community.html#mailing-lists)
- [Spark JIRA](https://issues.apache.org/jira/projects/SPARK)

## Getting Help

- Ask questions on [user@spark.apache.org](mailto:user@spark.apache.org)
- Report bugs on [JIRA](https://issues.apache.org/jira/projects/SPARK)
- Discuss on [dev@spark.apache.org](mailto:dev@spark.apache.org)
- Chat on the [Spark Slack](https://spark.apache.org/community.html)

## Contributing Back

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed contribution guidelines.

Remember: Quality over quantity. Well-tested, documented changes are more valuable than large, poorly understood patches.
