# Test Application Builds
This directory includes test applications which are built when auditing releases. You can
run them locally by setting appropriate environment variables.

```
$ cd sbt_app_core
$ SCALA_VERSION=2.10.5 \
  SPARK_VERSION=1.0.0-SNAPSHOT \
  SPARK_RELEASE_REPOSITORY=file:///home/patrick/.ivy2/local \
  sbt run
```
