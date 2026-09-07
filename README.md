# Apache Spark

Spark is a unified analytics engine for large-scale data processing. It provides
high-level APIs in Scala, Java, Python, and R (Deprecated), and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing,
and Structured Streaming for stream processing.

- Official version: <https://spark.apache.org/>
- Development version: <https://apache.github.io/spark/>

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.spark/spark-core_2.13.svg?filter=!*preview*)](https://search.maven.org/search?q=g:org.apache.spark)
[![Java](https://img.shields.io/badge/Java-17+-orange.svg)](https://adoptium.net/temurin/releases/?version=17)
[![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_main.yml)
[![PySpark Coverage](https://codecov.io/gh/apache/spark/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/spark)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/pyspark?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads)](https://pypi.org/project/pyspark/)


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](https://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## Build Pipeline Status

| Branch     | Status                                                                                                                                                                                                          |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| master     | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/release.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/release.yml)                                               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/publish_snapshot.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/publish_snapshot.yml)                             |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_infra_images_cache.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_infra_images_cache.yml)             |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_java21.yml)                                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java25.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_java25.yml)                                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_codegen_jdk.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_codegen_jdk.yml)                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_uds.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_uds.yml)                                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_rockdb_as_ui_backend.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_rockdb_as_ui_backend.yml)         |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven.yml)                                       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml)                         |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java25.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java25.yml)                         |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21_macos26.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java21_macos26.yml)         |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21_arm.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_maven_java21_arm.yml)                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_coverage.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_coverage.yml)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml)                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.12_classic_only.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.12_classic_only.yml) |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.12_arm.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.12_arm.yml)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.12_macos26.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.12_macos26.yml)           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.12_pandas_3.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.12_pandas_3.yml)         |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.13.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.13.yml)                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml)                           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14_nogil.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_3.14_nogil.yml)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_minimum.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_minimum.yml)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_connect40.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_connect40.yml)                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_connect.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_python_connect.yml)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_sparkr_window.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_sparkr_window.yml)                       |
| branch-4.x | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.x)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.x)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.x)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java25.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_java25.yml?query=branch%3Abranch-4.x)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.x)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.x)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.x)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.x)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg?branch=branch-4.x)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml?query=branch%3Abranch-4.x)           |
| branch-4.3 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.3)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.3)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.3)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java25.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_java25.yml?query=branch%3Abranch-4.3)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.3)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.3)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.3)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.3)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg?branch=branch-4.3)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml?query=branch%3Abranch-4.3)           |
| branch-4.2 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.2)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.2)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.2)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java25.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_java25.yml?query=branch%3Abranch-4.2)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.2)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.2)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.2)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.2)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg?branch=branch-4.2)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml?query=branch%3Abranch-4.2)           |
| branch-4.1 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.1)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.1)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.1)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.1)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.1)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.1)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.1)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_python_3.14.yml?query=branch%3Abranch-4.1)           |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml/badge.svg?branch=branch-4.1)](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml?query=branch%3Abranch-4.1) |
| branch-4.0 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-4.0)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java17.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_java17.yml?query=branch%3Abranch-4.0)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_java21.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_java21.yml?query=branch%3Abranch-4.0)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_non_ansi.yml?query=branch%3Abranch-4.0)               |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_maven.yml?query=branch%3Abranch-4.0)                     |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_maven_java21.yml?query=branch%3Abranch-4.0)       |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_python_3.11.yml?query=branch%3Abranch-4.0)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml/badge.svg?branch=branch-4.0)](https://github.com/apache/spark/actions/workflows/build_python_pypy3.10.yml?query=branch%3Abranch-4.0) |
| branch-3.5 | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg?branch=branch-3.5)](https://github.com/apache/spark/actions/workflows/build_main.yml?query=branch%3Abranch-3.5)                                 |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_scala213.yml/badge.svg?branch=branch-3.5)](https://github.com/apache/spark/actions/workflows/build_scala213.yml?query=branch%3Abranch-3.5)                   |
|            | [![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_python_3.9.yml/badge.svg?branch=branch-3.5)](https://github.com/apache/spark/actions/workflows/build_python_3.9.yml?query=branch%3Abranch-3.5)                   |


## Building Spark

Spark is built using [Apache Maven](https://maven.apache.org/).
To build Spark and its example programs, run:

```bash
./build/mvn -DskipTests clean package
```

(You do not need to do this if you downloaded a pre-built package.)

More detailed documentation is available from the project site, at
["Building Spark"](https://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see ["Useful Developer Tools"](https://spark.apache.org/developer-tools.html).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

```bash
./bin/spark-shell
```

Try the following command, which should return 1,000,000,000:

```scala
scala> spark.range(1000 * 1000 * 1000).count()
```

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

```bash
./bin/pyspark
```

And run the following command, which should also return 1,000,000,000:

```python
>>> spark.range(1000 * 1000 * 1000).count()
```

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

```bash
./bin/run-example SparkPi
```

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

```bash
MASTER=spark://host:7077 ./bin/run-example SparkPi
```

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

```bash
./dev/run-tests
```

Please see the guidance on how to
[run tests for a module, or individual tests](https://spark.apache.org/developer-tools.html#individual-tests).

There is also a Kubernetes integration test, see resource-managers/kubernetes/integration-tests/README.md

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version and Enabling YARN"](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.


## 🌐 Web Resources & Interactive Index
- [CONTACT](https://learnaction.netlify.app/contact.html)
- [GOOBER SHOT](https://eduquestspt.pages.dev/goober-shot.html)
- [FORTRESS OF THE SINISTER](https://eduquestsjp.pages.dev/fortress-of-the-sinister.html)
- [CATEGORY WAR](https://eduquestses.pages.dev/category-war.html)
- [WRECK THE TOWER](https://eduquestses.pages.dev/wreck-the-tower.html)
- [FALL BEAN 2](https://eduquestses.pages.dev/fall-bean-2.html)
- [COUNT MASTER MATCH COLOR RUN](https://eduquestspt.pages.dev/count-master-match-color-run.html)
- [ROLLER COASTER 3D](https://eduquestspt.pages.dev/roller-coaster-3d.html)
- [CATEGORY MINING75](https://eduquestsfr.pages.dev/category-mining75.html)
- [AGENT ZERO INFILTRATION](https://eduquestses.pages.dev/agent-zero-infiltration.html)
- [2 PLAYER MINI CHALLENGE](https://eduquestspt.pages.dev/2-player-mini-challenge.html)
- [CATEGORY INCREMENTAL388](https://eduquestsfr.pages.dev/category-incremental388.html)
- [CATEGORY MOBILE2 112 2](https://eduquestsfr.pages.dev/category-mobile2-112-2.html)
- [ROPE KING](https://eduquestspt.pages.dev/rope-king.html)
- [CATEGORY MERGE TITLES](https://eduquestses.pages.dev/category-merge-titles.html)
- [HEXAGON](https://eduquestspt.pages.dev/hexagon.html)
- [KAWAII CLAW MERGE](https://eduquestspt.pages.dev/kawaii-claw-merge.html)
- [TOWER OF FALL](https://eduquestspt.pages.dev/tower-of-fall.html)
- [CATEGORY OBSTACLE299](https://eduquestsfr.pages.dev/category-obstacle299.html)
- [CATEGORY STICKMAN](https://eduquestses.pages.dev/category-stickman.html)
- [BUTTERFLY KYODAI RAINBOW](https://eduquestses.pages.dev/butterfly-kyodai-rainbow.html)
- [JIGSAW FANTASY](https://eduquestses.pages.dev/jigsaw-fantasy.html)
- [INDEX40](https://eduquestsfr.pages.dev/index40.html)
- [WORLD WAR BROTHERS WW2](https://eduquestses.pages.dev/world-war-brothers-ww2.html)
- [FROG BYTE](https://eduquestses.pages.dev/frog-byte.html)
- [GT CHAMPIONSHIP ARCADE](https://eduquestspt.pages.dev/gt-championship-arcade.html)
- [COLOR WOOD ANIMAL JAM](https://eduquestspt.pages.dev/color-wood-animal-jam.html)
- [BRAINROT EVOLUTION GAME](https://eduquestses.pages.dev/brainrot-evolution-game.html)
- [TRAFFIC LIGHT SIMULATOR 3D](https://eduquestses.pages.dev/traffic-light-simulator-3d.html)
- [FISH OUT OF WATER](https://eduquestspt.pages.dev/fish-out-of-water.html)
- [MAHJONG LINES](https://eduquestses.pages.dev/mahjong-lines.html)
- [WORDMEISTER HD](https://eduquestspt.pages.dev/wordmeister-hd.html)
- [MEGA FALL RAGDOLL SIMULATOR](https://eduquestses.pages.dev/mega-fall-ragdoll-simulator.html)
- [CATEGORY BLOCK91](https://eduquestsfr.pages.dev/category-block91.html)
- [SUPERHERO TRANSFORM CHANGE RACE](https://eduquestspt.pages.dev/superhero-transform-change-race.html)
- [CAR STUNT RACING 3D](https://eduquestspt.pages.dev/car-stunt-racing-3d.html)
- [WORDS MATCH](https://eduquestses.pages.dev/words-match.html)
- [FRUIT MERGE JUICY DROP GAME](https://eduquestses.pages.dev/fruit-merge-juicy-drop-game.html)
- [CATEGORY PUZZLE 9](https://eduquestses.pages.dev/category-puzzle-9.html)
- [CATEGORY TOP DOWN251](https://eduquestses.pages.dev/category-top-down251.html)
- [RESIDENT EVIL PURGE OPERATION](https://eduquestspt.pages.dev/resident-evil-purge-operation.html)
- [WITCHY SISTERS RELAX PUZZLE](https://eduquestspt.pages.dev/witchy-sisters-relax-puzzle.html)
- [CATEGORY WAR GAME](https://eduquestses.pages.dev/category-war-game.html)
- [SOKOBAN PUZZLE GAME](https://eduquestspt.pages.dev/sokoban-puzzle-game.html)
- [GUN RACING](https://eduquestses.pages.dev/gun-racing.html)
- [CATEGORY GROW99](https://eduquestsfr.pages.dev/category-grow99.html)
- [PUZZLEJAM](https://eduquestspt.pages.dev/puzzlejam.html)
- [HEX TRIPLE MATCH](https://eduquestsfr.pages.dev/hex-triple-match.html)
- [CATEGORY DESTROY256](https://eduquestses.pages.dev/category-destroy256.html)
- [CATEGORY BATTLE ROYALE25](https://eduquestsfr.pages.dev/category-battle-royale25.html)
- [CATEGORY MERGE 2](https://eduquestses.pages.dev/category-merge-2.html)
- [CATEGORY DRIFTING116](https://eduquestsfr.pages.dev/category-drifting116.html)
- [MAHJONG MAGIC ISLANDS](https://eduquestspt.pages.dev/mahjong-magic-islands.html)
- [CATEGORY HORROR](https://eduquestses.pages.dev/category-horror.html)
- [BUBBLE SHOOTER PRO 4](https://eduquestses.pages.dev/bubble-shooter-pro-4.html)
- [INDEX27](https://eduquestsfr.pages.dev/index27.html)
- [TWO ARCHERS BOW DUEL](https://eduquestspt.pages.dev/two-archers-bow-duel.html)
- [BIG HEAD](https://ieduquests.web.app/big-head.html)
- [FLIGHT SIM AIR TRAFFIC CONTROL](https://learnaction.netlify.app/flight-sim-air-traffic-control.html)
- [CATEGORY TOP DOWN251](https://learnaction.netlify.app/category-top-down251.html)
- [MATH KING MATH SKILL GAME](https://eduquestspt.pages.dev/math-king-math-skill-game.html)
- [CATEGORY IBOSS](https://eduquests.pages.dev/category-iboss.html)
- [ZOMBIE HORDE BUILD SURVIVE](https://welearnaction.onrender.com/zombie-horde-build-survive.html)
- [ALIEN INTELLIGENCE TEST](https://eduquestspt.pages.dev/alien-intelligence-test.html)
- [CATEGORY AIRPLANE](https://eduquestsfr.pages.dev/category-airplane.html)
- [SCHOOL SIMULATOR MY SCHOOL](https://eduquestspt.pages.dev/school-simulator-my-school.html)
- [SIEGE BREAK](https://eduquestses.pages.dev/siege-break.html)
- [COLOR COCKTAIL](https://ieduquests.web.app/color-cocktail.html)
- [CAT LIFE SIMULATOR](https://eduquestses.pages.dev/cat-life-simulator.html)
- [THE BRANCH RUNNER](https://eduquests.pages.dev/the-branch-runner.html)
- [CATEGORY DRESS UP97](https://eduquestses.pages.dev/category-dress-up97.html)
- [FAR ORION NEW WORLDS](https://eduquestspt.pages.dev/far-orion-new-worlds.html)
- [COLORWARSIO CONQUEST GAME](https://eduquestses.pages.dev/colorwarsio-conquest-game.html)
- [MR LONG LEGS](https://learnaction.netlify.app/mr-long-legs.html)
- [CITY BIKE RACING CHAMPION](https://eduquestsfr.pages.dev/city-bike-racing-champion.html)
- [MERGE CHRISTMAS](https://eduquests.github.io/merge-christmas.html)
- [CATEGORY MMO24](https://learnaction.github.io/category-mmo24.html)
- [FIND THE DIFFERENCES CARS](https://eduquests.netlify.app/find-the-differences-cars.html)
- [CATEGORY UNBLOCKED GAMES](https://eduquests.github.io/category-unblocked-games.html)
- [BOAT GAME RACING SIMULATOR 3D](https://learnaction.netlify.app/boat-game-racing-simulator-3d.html)
- [MUTANT ASSASSIN 3D](https://learnaction.netlify.app/mutant-assassin-3d.html)
- [ITALIAN BRAINROT PUZZLE BATTLE](https://eduquests.pages.dev/italian-brainrot-puzzle-battle.html)
- [PIXEL MINI GOLF](https://eduquests.pages.dev/pixel-mini-golf.html)
- [PIXEL BLAST](https://eduquests.pages.dev/pixel-blast.html)
- [BULL RUNNER](https://learnaction.github.io/bull-runner.html)
- [STICKER BOOK PUZZLE COLOR BY NUMBER](https://welearnaction.onrender.com/sticker-book-puzzle-color-by-number.html)
- [CATEGORY 2D1 070](https://eduquests.netlify.app/category-2d1-070.html)
- [MY COTTAGECORE AESTHETIC LOOK](https://ieduquests.web.app/my-cottagecore-aesthetic-look.html)
- [AHA WORLD DREAM TOWN](https://eduquests.pages.dev/aha-world-dream-town.html)
- [CATEGORY PUZZLE 6](https://ieduquests.web.app/category-puzzle-6.html)
- [SORT WORKS NUTS ORDER](https://eduquests.pages.dev/sort-works-nuts-order.html)
- [WORM HUNT](https://eduquestsfr.pages.dev/worm-hunt.html)
- [CATEGORY CASUAL 5](https://learnaction.netlify.app/category-casual-5.html)
- [FORMULA RACING GAMES CAR GAME](https://ieduquests.web.app/formula-racing-games-car-game.html)
- [BUBBLE SHOOTER WITCH TOWER 2](https://eduquests.pages.dev/bubble-shooter-witch-tower-2.html)
- [TROPICAL MATCH 2](https://eduquestses.pages.dev/tropical-match-2.html)
- [SLINGSHOT FORTRESS](https://eduquests.pages.dev/slingshot-fortress.html)
- [TERMS](https://themindplays.pages.dev/terms.html)
- [SANDBOX ISLAND WAR](https://ieduquests.web.app/sandbox-island-war.html)
- [SNAKE GO ESCAPE PUZZLE](https://ieduquests.web.app/snake-go-escape-puzzle.html)
- [MAGIC SORT](https://eduquests.pages.dev/magic-sort.html)
- [VEX HYPER DASH](https://eduquestspt.pages.dev/vex-hyper-dash.html)
- [CATEGORY FIGHTING124](https://eduquestses.pages.dev/category-fighting124.html)
- [CATEGORY SNIPER39](https://learnaction.netlify.app/category-sniper39.html)
- [BEAUTY WORLD AND FASHION STYLIST](https://learnaction.netlify.app/beauty-world-and-fashion-stylist.html)
- [THE SUPERHERO LEAGUE](https://eduquests.onrender.com/the-superhero-league.html)
- [UNTANGLE RINGS MASTER](https://welearnaction.onrender.com/untangle-rings-master.html)
- [KIKI WORLD KAWAII DOLL DECOR](https://ieduquests.web.app/kiki-world-kawaii-doll-decor.html)
- [SHIP CONTROL 3D](https://eduquestses.pages.dev/ship-control-3d.html)
- [JUMP UP 3D BASKETBALL GAME](https://ieduquests.web.app/jump-up-3d-basketball-game.html)
- [FALL BEAN 2](https://eduquests.pages.dev/fall-bean-2.html)
- [PUZZLE BLOCKS](https://eduquestspt.pages.dev/puzzle-blocks.html)
- [BRAINROT MERGE](https://eduquestses.pages.dev/brainrot-merge.html)
- [FURRY KUNG FU](https://eduquests.github.io/furry-kung-fu.html)
- [TETRO MERGE](https://learnaction.netlify.app/tetro-merge.html)
- [FOOTBALL SUPERSTARS 2026](https://eduquests.onrender.com/football-superstars-2026.html)
- [FROGGY HOP](https://eduquestsfr.pages.dev/froggy-hop.html)
- [GANG WAR STRIKE SHOOTER](https://learnaction.github.io/gang-war-strike-shooter.html)
- [MONSTER SCHOOL VS SIREN HEAD](https://eduquestses.pages.dev/monster-school-vs-siren-head.html)
- [MONEY MAN 3D](https://eduquests.pages.dev/money-man-3d.html)
- [SLAP AND RUN](https://eduquests.pages.dev/slap-and-run.html)
- [OBBY HIGHEST JUMP EVER](https://eduquestses.pages.dev/obby-highest-jump-ever.html)
- [SHIP FACTORY TYCOON](https://eduquestses.pages.dev/ship-factory-tycoon.html)
- [INDEX12](https://ieduquests.web.app/index12.html)
- [PRINCESS RUN 3D](https://eduquests.pages.dev/princess-run-3d.html)
- [MOBILE LEGENDS SLIME 3V3](https://eduquestsfr.pages.dev/mobile-legends-slime-3v3.html)
- [STICKMAN DUO ESCAPE THE TOMB](https://eduquests.onrender.com/stickman-duo-escape-the-tomb.html)
- [INCREDIBLE KIDS DENTIST](https://eduquestspt.pages.dev/incredible-kids-dentist.html)
- [THEME WORD SEARCH](https://learnaction.github.io/theme-word-search.html)
- [CATEGORY MAKEUP51](https://eduquestsfr.pages.dev/category-makeup51.html)
