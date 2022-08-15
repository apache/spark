---
layout: global
title: Spark Hadoop3 Integration Tests
---

# Running the Integration Tests

As mocking of an external systems (like AWS S3) is not always perfect the unit testing should be
extended with integration testing. This is why the build profile `integration-test` has been
introduced here. When it is given (`-Pintegration-test`) for testing then only those tests are
executed where the `org.apache.spark.internal.io.cloud.IntegrationTestSuite` tag is used.

One example is `AwsS3AbortableStreamBasedCheckpointFileManagerSuite`.

Integration tests will have some extra configurations for example selecting the external system to
run the test against. Those configs are passed as environment variables and the existence of these
variables must be checked by the test.
Like for `AwsS3AbortableStreamBasedCheckpointFileManagerSuite` the S3 bucket used for testing
is passed in the `S3A_PATH` and the credetinals to access AWS S3 are AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY (in addition you can define an optional AWS_SESSION_TOKEN too).
