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
package org.apache.spark.security.aws.integrationtest

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.{Base64, UUID}

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.rbac.{PolicyRuleBuilder, RoleBindingBuilder, RoleBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import org.scalatest.Tag
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Seconds, Span}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

/**
 * End-to-end integration test for OIDC credential propagation with the AWS reference provider.
 *
 * Prerequisites (handled by the GitHub Actions workflow or dev-run-integration-tests.sh,
 * or set up manually):
 *  - Minikube running
 *  - moto server running, reachable from the test JVM (loopback) and from Minikube pods
 *    (host gateway IP)
 *  - A Spark image built with `-Phadoop-cloud` (for S3A) and the module jar baked into
 *    `/opt/spark/jars` (for the job classes)
 *
 * Activate with:
 * {{{
 *   mvn -Pkubernetes -Pcredential-aws -Poidc-e2e integration-test \
 *     -pl connector/credential-aws-integration-tests
 * }}}
 *
 * System properties (all have defaults; override via Maven `-D` flags or pom.xml properties):
 *  - spark.kubernetes.test.imageRepo  - Docker image repository
 *  - spark.kubernetes.test.imageTag   - Docker image tag
 *  - spark.oidc.test.stsEndpoint      - moto STS endpoint as seen from pods (host gateway IP)
 *  - spark.oidc.test.s3Endpoint       - moto S3 endpoint as seen from pods (host gateway IP)
 *  - spark.oidc.test.s3ClientEndpoint - moto S3 endpoint as seen from the test JVM (loopback)
 *  - spark.oidc.test.roleArn          - IAM role ARN for AssumeRoleWithWebIdentity
 *  - spark.oidc.test.tokenFile        - path to the OIDC token file inside the driver pod
 *  - spark.oidc.test.s3Bucket         - S3 bucket name to use in moto
 *  - spark.oidc.test.sparkImage       - full Spark image name (overrides repo+tag)
 */
class OidcCredentialE2ESuite
    extends SparkFunSuite
    with Matchers
    with Eventually {

  import OidcCredentialE2ESuite._

  // -------------------------------------------------------------------------
  // Configuration read from system properties (injected by pom.xml)
  // -------------------------------------------------------------------------

  private val imageRepo: String =
    prop("spark.kubernetes.test.imageRepo", "docker.io/kubespark")
  private val imageTag: String =
    // Sentinel default mirrors pom.xml and kubernetes-integration-tests: "N/A" means
    // no concrete image was configured. sparkImage below fails fast on this value
    // rather than constructing an unpullable "spark:N/A" reference.
    prop("spark.kubernetes.test.imageTag", "N/A")
  private val sparkImage: String = {
    val explicit = prop("spark.oidc.test.sparkImage", "")
    if (explicit.nonEmpty) {
      explicit
    } else {
      // Fall back to imageRepo/spark:imageTag only when a concrete tag is available.
      // The pom.xml default for imageTag is the sentinel "N/A" (mirroring
      // kubernetes-integration-tests), which does not name a real image. Building
      // "$imageRepo/spark:N/A" here would defer the failure to an opaque
      // ImagePullBackOff on the driver pod. Fail fast instead with an actionable
      // message telling the caller how to provide an image.
      require(imageTag.nonEmpty && imageTag != "N/A",
        "No Spark test image configured. Set -Dspark.oidc.test.sparkImage to a full " +
          "image name (e.g. docker.io/kubespark/spark:my-tag), or set " +
          "-Dspark.kubernetes.test.imageTag (with -Dspark.kubernetes.test.imageRepo) " +
          "to a tag that has been built and loaded into the cluster. The " +
          "dev-run-integration-tests.sh script and the CI workflow set this for you.")
      s"$imageRepo/spark:$imageTag"
    }
  }
  private val namespace: String =
    prop("spark.kubernetes.test.namespace",
      s"oidc-e2e-${UUID.randomUUID().toString.take(8)}")
  private val serviceAccountName: String =
    prop("spark.kubernetes.test.serviceAccountName", "default")

  private val stsEndpoint: String =
    prop("spark.oidc.test.stsEndpoint", "http://localhost:5000")
  private val s3Endpoint: String =
    prop("spark.oidc.test.s3Endpoint", "http://localhost:5000")
  // Endpoint used by the test process (running on the host) to talk to moto for
  // setup/verification. This differs from [[s3Endpoint]]/[[stsEndpoint]], which are
  // consumed by the Spark driver/executor pods inside Minikube: pods reach moto via the
  // host gateway IP (e.g. 192.168.49.1), whereas the host itself reaches it on loopback.
  private val s3ClientEndpoint: String =
    prop("spark.oidc.test.s3ClientEndpoint", "http://127.0.0.1:5000")
  private val roleArn: String =
    prop("spark.oidc.test.roleArn",
      "arn:aws:iam::123456789012:role/oidc-e2e-test-role")
  private val tokenFile: String =
    prop("spark.oidc.test.tokenFile",
      "/var/run/secrets/kubernetes.io/serviceaccount/token")
  private val s3Bucket: String =
    prop("spark.oidc.test.s3Bucket", "oidc-e2e-test-bucket")

  /**
   * Reads a system property, treating an unset property, an empty string, and the
   * literal string "null" as "not configured" (returning `default`). The last two
   * cases matter because pom.xml declares several of these properties with empty
   * default values (e.g. `<spark.kubernetes.test.namespace></spark.kubernetes.test.namespace>`).
   * When Maven's scalatest plugin forwards an empty property it arrives in the JVM as
   * the string "null", not "", so a plain getOrElse would use "null" verbatim. sbt does
   * not set these properties at all, so this normalization keeps Maven and sbt behaviour
   * identical.
   */
  private def prop(key: String, default: String): String = {
    sys.props.get(key) match {
      case Some(v) if v.nonEmpty && v != "null" => v
      case _ => default
    }
  }

  // -------------------------------------------------------------------------
  // Infrastructure
  // -------------------------------------------------------------------------

  private var kubernetesClient: KubernetesClient = _
  private var s3Client: S3Client = _
  private var createdNamespace: Boolean = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    kubernetesClient = new KubernetesClientBuilder().build()
    s3Client = buildS3Client()
    ensureNamespace()
    ensureMotoResources()
  }

  override def afterAll(): Unit = {
    try {
      // Wrap each teardown step independently so a failure in one (e.g. the namespace
      // delete) does not skip the others (closing the S3 and Kubernetes clients).
      if (createdNamespace) {
        Utils.tryLogNonFatalError {
          val namespaces = kubernetesClient.namespaces()
          namespaces.withName(namespace).delete()
          // Wait for the namespace to be fully gone; otherwise an immediate re-run with
          // a fixed -Dspark.kubernetes.test.namespace could find it still Terminating,
          // treat it as pre-existing, and fail on submit.
          val deadline = System.currentTimeMillis() + DELETE_TIMEOUT_MS
          while (namespaces.withName(namespace).get() != null &&
              System.currentTimeMillis() < deadline) {
            Thread.sleep(INTERVAL.value.toMillis)
          }
        }
      }
      Utils.tryLogNonFatalError(Option(s3Client).foreach(_.close()))
      Utils.tryLogNonFatalError(Option(kubernetesClient).foreach(_.close()))
    } finally {
      super.afterAll()
    }
  }

  // -------------------------------------------------------------------------
  // Test cases
  // -------------------------------------------------------------------------

  /**
   * Test 1: Basic OIDC credential propagation flow.
   *
   * Scenario:
   *  1. A Spark job runs on Minikube with spark.security.oidc.enabled=true.
   *  2. The driver reads the Projected SA token from [[tokenFile]].
   *  3. AwsStsCredentialProvider calls moto's AssumeRoleWithWebIdentity.
   *  4. The resulting credentials are propagated to executors.
   *  5. Executors write a file to S3 (moto) via S3A using the propagated credentials.
   *  6. The test verifies the file exists in moto S3.
   */
  test("OIDC credential propagation: basic S3 read/write on Minikube", oidcE2eTag) {
    val outputPath = s"s3a://$s3Bucket/e2e-basic-${UUID.randomUUID().toString.take(8)}/"
    // A fixed executor count (this test does not use dynamic allocation).
    val conf = baseSparkConf().set("spark.executor.instances", "1")

    runOidcJobAndVerify(conf, outputPath)
  }

  /**
   * Test 2: Mid-job token rotation triggers credential refresh.
   *
   * Scenario:
   *  1. A long-running Spark job ([[OidcTokenRotationJob]]) starts with an initial
   *     identity token supplied by an init container into an emptyDir (simulating an
   *     externally-provided, rotatable token file such as a K8s projected SA token).
   *  2. Mid-job, the test rewrites the token file in the driver pod (via `kubectl exec`
   *     equivalent through the fabric8 client) with a token carrying a DIFFERENT
   *     principal (subject), simulating an external token rotation.
   *  3. UserCredentialManager re-reads the rotated token on its next (short-interval)
   *     renewal, exchanges it via STS, and propagates fresh credentials to executors.
   *  4. The test asserts the driver actually consumed the rotated token by matching the
   *     new principal in the driver log ("Loaded identity token for principal <rotated>").
   *     This distinguishes a real rotation from a no-op (the initial credentials would
   *     otherwise remain valid for moto's long STS TTL, so "all iterations wrote" alone
   *     would NOT prove the rotated token was used).
   *  5. The job keeps writing to S3 across the rotation boundary; the test verifies that
   *     objects for iterations spanning the rotation all exist.
   */
  test("OIDC credential propagation: mid-job token rotation triggers refresh", oidcE2eTag) {
    val outputPrefix = s"s3a://$s3Bucket/e2e-rotation-${UUID.randomUUID().toString.take(8)}"
    val iterations = 8
    val sleepMillis = 5000L

    val driverPodName =
      s"oidc-rot-${UUID.randomUUID().toString.replaceAll("-", "").take(16)}-driver"

    // Use distinct principals for the initial and rotated tokens so we can prove, from
    // the driver log, that the rotated token was actually read (not a no-op).
    val initialSubject = s"system:serviceaccount:$namespace:$serviceAccountName"
    val rotatedSubject = s"system:serviceaccount:$namespace:rotated-oidc-principal"

    // Initial identity token placed by the init container into /oidc/token.
    val initialToken = makeUnsignedJwt(initialSubject, ttlSeconds = 600)
    val podTemplatePath = writeDriverPodTemplate(initialToken)

    val conf = baseSparkConf()
      .set("spark.kubernetes.driver.pod.name", driverPodName)
      // A fixed executor count (this test does not use dynamic allocation).
      .set("spark.executor.instances", "1")
      .set("spark.kubernetes.driver.podTemplateFile", podTemplatePath.toString)
      // The identity token now comes from the init-container-populated emptyDir.
      .set("spark.security.oidc.identityToken.file", "/oidc/token")
      // Renew frequently so the rotated token is picked up quickly.
      .set("spark.security.oidc.renewal.minInterval", "3s")
      .set("spark.security.oidc.renewal.safetyMargin", "590s")
      // Do not block: the test needs to rotate the token while the job runs.
      .set("spark.kubernetes.submission.waitAppCompletion", "false")

    val appArguments = SparkAppArguments(
      mainAppResource = jobJarResource,
      mainClass = ROTATION_JOB_MAIN_CLASS,
      appArgs = Array(s"$outputPrefix/", iterations.toString, sleepMillis.toString))

    try {
      // Launch the (non-blocking) submit. Inside the try so a launch failure still runs
      // the finally (driver pod + temp pod-template cleanup).
      SparkAppLauncher.launch(
        appArguments, conf, TIMEOUT.value.toSeconds.toInt, resolveSparkHomeDir())

      // Wait until the driver has acquired the initial credentials and the job has
      // produced output on both sides of the rotation. Rotate after an early iteration
      // so that several iterations still remain after the rotation. Fails fast if the
      // driver pod enters the terminal Failed phase.
      awaitDriverLogContains(driverPodName, Timeout(Span(3, Minutes)),
        "Credential acquisition successful", "Iteration 1:")

      // Rotate the token: overwrite /oidc/token in the running driver pod with a token
      // carrying a DIFFERENT principal.
      val rotatedToken = makeUnsignedJwt(rotatedSubject, ttlSeconds = 600)
      rewriteDriverTokenFile(driverPodName, rotatedToken)
      logInfo(s"Rotated identity token in driver pod (new principal: $rotatedSubject).")

      // Authoritative check that the rotated token was actually consumed: the driver's
      // renewal loop logs the principal it just loaded. Seeing the rotated principal
      // proves the new token file was re-read and exchanged (not a no-op). Fails fast on
      // a terminal Failed phase.
      awaitDriverLogContains(driverPodName, Timeout(Span(2, Minutes)),
        s"Loaded identity token for principal $rotatedSubject")

      // Wait for the job to finish successfully (fail fast on a terminal Failed phase).
      awaitDriverSucceeded(driverPodName, VERIFY_TIMEOUT)

      // Verify all iterations wrote output (i.e. S3 access continued across rotation).
      // iterations is small (< 1000), so a single listObjectsV2 page is sufficient.
      val prefix = s"$outputPrefix/".stripPrefix(s"s3a://$s3Bucket/")
      val objects = s3Client.listObjectsV2(
        ListObjectsV2Request.builder().bucket(s3Bucket).prefix(prefix).build())
      val keys = objects.contents().asScala.map(_.key())
      for (i <- 0 until iterations) {
        assert(keys.exists(_.contains(s"iter-$i/")),
          s"Missing S3 output for iteration $i (rotation may have interrupted access). " +
            s"Found keys: ${keys.mkString(", ")}")
      }
    } catch {
      case e: Throwable =>
        dumpDriverDiagnostics(driverPodName)
        throw e
    } finally {
      deleteDriverPod(driverPodName)
      Files.deleteIfExists(podTemplatePath)
    }
  }

  /**
   * Test 3: Late-registering executor receives credentials.
   *
   * Scenario:
   *  1. A Spark job ([[OidcLateExecutorJob]]) starts with dynamic allocation enabled and
   *     runs a small warm-up stage so the driver acquires credentials early.
   *  2. The job idles long enough for dynamic allocation to release idle executors.
   *  3. A wider second stage forces new executors to be requested and registered *after*
   *     credentials were already acquired. Each task in that stage writes to S3.
   *  4. If every task succeeds (verified by the presence of all wide-stage outputs and a
   *     Succeeded pod phase), the late-registering executors received credentials via the
   *     SparkAppConfig registration response, without waiting for the next renewal.
   */
  test("OIDC credential propagation: late-registering executor receives credentials",
      oidcE2eTag) {
    val outputPrefix = s"s3a://$s3Bucket/e2e-late-exec-${UUID.randomUUID().toString.take(8)}"
    val idleMillis = 30000L
    val partitions = 4

    val driverPodName =
      s"oidc-late-${UUID.randomUUID().toString.replaceAll("-", "").take(16)}-driver"

    val conf = baseSparkConf()
      .set("spark.kubernetes.driver.pod.name", driverPodName)
      // Dynamic allocation with shuffle tracking (no external shuffle service on K8s).
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
      .set("spark.dynamicAllocation.minExecutors", "0")
      .set("spark.dynamicAllocation.maxExecutors", "3")
      .set("spark.dynamicAllocation.initialExecutors", "1")
      // Scale idle executors down quickly so the wide stage must request new ones.
      .set("spark.dynamicAllocation.executorIdleTimeout", "5s")
      .set("spark.dynamicAllocation.shuffleTracking.timeout", "5s")
      // Do not block: the test observes the driver's progress (and late executor
      // registration) through the pod log while the job runs.
      .set("spark.kubernetes.submission.waitAppCompletion", "false")

    val appArguments = SparkAppArguments(
      mainAppResource = jobJarResource,
      mainClass = LATE_EXECUTOR_JOB_MAIN_CLASS,
      appArgs = Array(s"$outputPrefix/", idleMillis.toString, partitions.toString))

    try {
      // Non-blocking submit; observe progress through the driver pod log.
      SparkAppLauncher.launch(
        appArguments, conf, TIMEOUT.value.toSeconds.toInt, resolveSparkHomeDir())

      // Wait until the warm-up stage completed and the driver has acquired credentials.
      // Fails fast if the driver pod enters the terminal Failed phase.
      awaitDriverLogContains(driverPodName, Timeout(Span(3, Minutes)),
        "Credential acquisition successful", "Warm-up stage complete")

      // After the idle period, the wide stage forces executors to be (re-)requested.
      // Wait for the job to report success. The generous timeout tolerates the timing
      // variability of dynamic allocation scaling executors down and back up on a
      // resource-constrained CI runner; a terminal Failed phase still fails fast.
      awaitDriverLogContains(driverPodName, TIMEOUT, OidcLateExecutorJob.SUCCESS_MARKER)

      // Confirm the driver pod finished successfully (fail fast on terminal Failed).
      awaitDriverSucceeded(driverPodName, VERIFY_TIMEOUT)

      // Prove the scenario actually exercised late-registering executors: more than one
      // distinct executor must have registered over the run. With initialExecutors=1 and
      // a short idle timeout, a second (or later) distinct executor ID only appears if
      // dynamic allocation scaled down and then registered a NEW executor for the wide
      // stage -- i.e. an executor that registered AFTER the initial credential
      // acquisition. A single long-lived executor would yield only one ID and fail here.
      val execIds = registeredExecutorIds(driverLog(driverPodName)).distinct
      assert(execIds.size >= 2,
        s"Expected more than one distinct executor to register (evidence of a " +
          s"late-registering executor after scale-down); saw IDs: ${execIds.mkString(", ")}")

      // Every wide-stage partition must have produced output; a late-registering executor
      // without credentials would have failed its task and the job.
      val widePrefix = s"$outputPrefix/wide/".stripPrefix(s"s3a://$s3Bucket/")
      val objects = s3Client.listObjectsV2(
        ListObjectsV2Request.builder().bucket(s3Bucket).prefix(widePrefix).build())
      val keys = objects.contents().asScala.map(_.key())
      // Spark writes part-* files plus a _SUCCESS marker for a successful save.
      assert(keys.exists(_.contains("_SUCCESS")),
        s"Wide stage did not complete successfully. Found keys: ${keys.mkString(", ")}")
      assert(keys.exists(_.contains("part-")),
        s"Wide stage produced no part files. Found keys: ${keys.mkString(", ")}")
    } catch {
      case e: Throwable =>
        dumpDriverDiagnostics(driverPodName)
        throw e
    } finally {
      deleteDriverPod(driverPodName)
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def baseSparkConf(): SparkAppConf = {
    // Derive the master from the fabric8 client (which follows the active kubeconfig)
    // rather than a separate, un-normalized property. This keeps spark-submit and the
    // fabric8 client (used here for pod/namespace/exec operations) pointed at the same
    // cluster, and avoids feeding SparkSubmit a raw "https://..." that it would reject.
    val masterUrl = s"k8s://${kubernetesClient.getMasterUrl}"

    new SparkAppConf()
      .set("spark.master", masterUrl)
      .set("spark.kubernetes.namespace", namespace)
      .set("spark.kubernetes.container.image", sparkImage)
      .set("spark.kubernetes.authenticate.driver.serviceAccountName", serviceAccountName)
      .set("spark.executor.cores", "1")
      .set("spark.kubernetes.driver.request.cores", "0.2")
      .set("spark.kubernetes.executor.request.cores", "0.2")
      // Make spark-submit block until the driver finishes so the test can detect
      // job failures immediately (instead of only polling S3 and timing out), and
      // surface driver progress in the submit output.
      .set("spark.kubernetes.submission.waitAppCompletion", "true")
      .set("spark.kubernetes.report.interval", "5s")
      // OIDC credential propagation
      .set("spark.security.oidc.enabled", "true")
      .set("spark.security.oidc.identityToken.file", tokenFile)
      .set("spark.security.oidc.aws.roleArn", roleArn)
      .set("spark.security.oidc.aws.stsEndpoint", stsEndpoint)
      .set("spark.security.oidc.aws.region", "us-east-1")
      // S3A configuration pointing to moto
      .set("spark.hadoop.fs.s3a.endpoint", s3Endpoint)
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      // Explicitly select the OIDC credential provider for S3A. The provider is
      // registered by the credential-aws module; setting it explicitly avoids
      // relying on defaulting behaviour and makes the test intent clear.
      .set("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.spark.security.aws.SparkOidcAwsCredentialsProvider")
  }

  private def runOidcJobAndVerify(conf: SparkAppConf, outputPath: String): Unit = {
    val driverPodName =
      s"oidc-e2e-${UUID.randomUUID().toString.replaceAll("-", "").take(16)}-driver"
    conf.set("spark.kubernetes.driver.pod.name", driverPodName)

    val appArguments = SparkAppArguments(
      mainAppResource = jobJarResource,
      mainClass = OIDC_JOB_MAIN_CLASS,
      appArgs = Array(outputPath))

    try {
      try {
        SparkAppLauncher.launch(
          appArguments,
          conf,
          TIMEOUT.value.toSeconds.toInt,
          resolveSparkHomeDir())
      } catch {
        case e: Throwable =>
          // A non-zero spark-submit exit means submission itself failed. Note that a
          // zero exit does NOT imply the driver succeeded: with waitAppCompletion=true
          // the submission client returns 0 once the driver reaches any terminal phase,
          // including Failed (LoggingPodStatusWatcherImpl.hasCompleted is true for both
          // Succeeded and Failed). The driver-phase and success-marker checks below are
          // what actually assert success. Dump diagnostics either way.
          dumpDriverDiagnostics(driverPodName)
          throw e
      }

      // spark-submit's exit code does not distinguish Succeeded from Failed, so assert
      // the driver reached Succeeded (fails fast on Failed) and that the job logged its
      // success marker before verifying S3 output.
      try {
        awaitDriverSucceeded(driverPodName, VERIFY_TIMEOUT)
        assert(driverLog(driverPodName).contains(OidcS3ReadWriteJob.SUCCESS_MARKER),
          s"Driver pod $driverPodName succeeded but did not log the success marker " +
            s"'${OidcS3ReadWriteJob.SUCCESS_MARKER}'.")
      } catch {
        case e: Throwable =>
          dumpDriverDiagnostics(driverPodName)
          throw e
      }

      // The driver has completed successfully, so the output should already be in moto.
      // Poll briefly to absorb any eventual consistency, and dump diagnostics if it
      // never shows up.
      try {
        Eventually.eventually(VERIFY_TIMEOUT, INTERVAL) {
          val objects = s3Client.listObjectsV2(
            ListObjectsV2Request.builder()
              .bucket(s3Bucket)
              .prefix(outputPath.stripPrefix(s"s3a://$s3Bucket/"))
              .build())
          objects.contents().asScala should not be empty
        }
      } catch {
        case e: Throwable =>
          dumpDriverDiagnostics(driverPodName)
          throw e
      }
    } finally {
      deleteDriverPod(driverPodName)
    }
  }

  /** Logs the driver pod's phase, describe-style status and container logs, best-effort. */
  private def dumpDriverDiagnostics(driverPodName: String): Unit = {
    try {
      val pod = kubernetesClient.pods().inNamespace(namespace).withName(driverPodName).get()
      if (pod == null) {
        logInfo(s"Driver pod $driverPodName not found in namespace $namespace.")
      } else {
        logInfo(s"Driver pod $driverPodName phase: ${pod.getStatus.getPhase}")
        logInfo(s"Driver pod status: ${pod.getStatus}")
      }
      val logs = kubernetesClient.pods().inNamespace(namespace)
        .withName(driverPodName).getLog()
      logInfo(s"===== Driver pod logs ($driverPodName) =====\n$logs")
    } catch {
      case t: Throwable =>
        logInfo(s"Failed to collect driver diagnostics for $driverPodName: ${t.getMessage}")
    }
  }

  /**
   * Best-effort deletion of the driver pod after a test. The suite runs several tests
   * in the same namespace with `waitAppCompletion=false`, so a driver (and its
   * executors, which Spark deletes via owner references when the driver goes away) left
   * running from one test would compete for the limited CPU/memory of the CI Minikube
   * and could destabilize the next test. Called from each test's `finally`.
   */
  private def deleteDriverPod(driverPodName: String): Unit = {
    Utils.tryLogNonFatalError {
      val pods = kubernetesClient.pods().inNamespace(namespace)
      pods.withName(driverPodName).delete()
      // Wait for the pod to actually disappear (delete() only requests deletion). This
      // keeps a still-terminating driver/executors from contending with the next test.
      val deadline = System.currentTimeMillis() + DELETE_TIMEOUT_MS
      while (pods.withName(driverPodName).get() != null &&
          System.currentTimeMillis() < deadline) {
        Thread.sleep(INTERVAL.value.toMillis)
      }
    }
  }

  /**
   * Waits until the driver log contains all of the given markers, failing fast if the
   * driver pod reaches the terminal "Failed" phase in the meantime, and failing on
   * timeout otherwise.
   *
   * This is used instead of ScalaTest's `eventually { assert(phase != "Failed"); ... }`
   * because `eventually` retries on ANY exception it catches -- including the
   * `assert(phase != "Failed")` failure -- so a dead pod would not fail fast; the block
   * would just keep retrying until the timeout expired. A hand-rolled poll loop that
   * throws (via `fail`) on the terminal phase gives the intended fail-fast behavior.
   */
  private def awaitDriverLogContains(
      driverPodName: String, timeout: Timeout, markers: String*): Unit = {
    val deadline = System.currentTimeMillis() + timeout.value.toMillis
    var found = false
    while (!found) {
      val phase = driverPodPhase(driverPodName)
      if (phase == "Failed") {
        fail(s"Driver pod $driverPodName reached terminal phase Failed while waiting " +
          s"for: ${markers.mkString(", ")}.")
      }
      val log = driverLog(driverPodName)
      if (markers.forall(log.contains)) {
        found = true
      } else if (System.currentTimeMillis() > deadline) {
        fail(s"Timed out waiting for driver log of $driverPodName to contain: " +
          s"${markers.filterNot(log.contains).mkString(", ")} (last phase: $phase).")
      } else {
        Thread.sleep(INTERVAL.value.toMillis)
      }
    }
  }

  /** The application resource (job jar) baked into the image at /opt/spark/jars. */
  private def jobJarResource: String = {
    // Derive the Scala binary version at runtime (e.g. "2.13") rather than hard-coding
    // it, so the jar name stays correct across Scala version bumps. Mirrors
    // kubernetes-integration-tests' Utils.getExamplesJarName().
    val scalaBinaryVersion = scala.util.Properties.versionNumberString
      .split("\\.")
      .take(2)
      .mkString(".")
    s"local:///opt/spark/jars/" +
      s"spark-credential-aws-integration-tests_$scalaBinaryVersion-${SPARK_VERSION}.jar"
  }

  /** Returns the current logs of the driver pod (best-effort, empty on error). */
  private def driverLog(driverPodName: String): String = {
    try {
      kubernetesClient.pods().inNamespace(namespace).withName(driverPodName).getLog()
    } catch {
      case _: Throwable => ""
    }
  }

  /** Returns the driver pod's phase, or "Unknown" if not found. */
  private def driverPodPhase(driverPodName: String): String = {
    val pod = kubernetesClient.pods().inNamespace(namespace).withName(driverPodName).get()
    if (pod == null || pod.getStatus == null) "Unknown" else pod.getStatus.getPhase
  }

  /**
   * Waits for the driver pod to reach a terminal phase, failing fast on "Failed"
   * instead of waiting for the whole timeout. Returns normally when the pod reaches
   * "Succeeded"; throws immediately on "Failed"; throws on timeout otherwise.
   */
  private def awaitDriverSucceeded(driverPodName: String, timeout: Timeout): Unit = {
    val deadline = System.currentTimeMillis() + timeout.value.toMillis
    var phase = driverPodPhase(driverPodName)
    while (phase != "Succeeded") {
      if (phase == "Failed") {
        fail(s"Driver pod $driverPodName reached terminal phase Failed.")
      }
      if (System.currentTimeMillis() > deadline) {
        fail(s"Timed out waiting for driver pod $driverPodName to succeed " +
          s"(last phase: $phase).")
      }
      Thread.sleep(INTERVAL.value.toMillis)
      phase = driverPodPhase(driverPodName)
    }
  }

  /**
   * Parses the distinct executor IDs that registered with the driver, in the order the
   * "Registered executor ... with ID <id>" lines appear in the driver log. Used by the
   * late-executor test to prove that more than one executor registered over the run
   * (i.e. dynamic allocation scaled down and back up), rather than a single long-lived
   * executor satisfying the assertions trivially.
   */
  private def registeredExecutorIds(driverLogText: String): Seq[String] = {
    val pattern = """Registered executor \S+ \([^)]*\) with ID (\S+?),""".r
    pattern.findAllMatchIn(driverLogText).map(_.group(1)).toSeq
  }

  /**
   * Builds a minimal unsigned JWT (alg:none) with the sub/iss/iat/exp claims that
   * FileTokenIngestor requires. moto does not verify the signature, and
   * FileTokenIngestor only Base64-decodes the payload, so an unsigned token is
   * sufficient for the test.
   *
   * The subject is parameterized so the rotation test can use a distinct principal
   * for the rotated token and then assert (via the driver log) that the driver
   * actually re-read the rotated token rather than continuing with the initial one.
   */
  private def makeUnsignedJwt(subject: String, ttlSeconds: Long): String = {
    def b64url(bytes: Array[Byte]): String =
      Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)
    val now = Instant.now().getEpochSecond
    val header = """{"alg":"none","typ":"JWT"}"""
    // A unique jti value ensures each rotation produces different token content,
    // so FileTokenIngestor detects the change even if the subject is unchanged.
    val payload =
      s"""{"sub":"$subject",""" +
        s""""iss":"https://kubernetes.default.svc.cluster.local",""" +
        s""""iat":$now,"exp":${now + ttlSeconds},"jti":"${UUID.randomUUID()}"}"""
    val h = b64url(header.getBytes(StandardCharsets.UTF_8))
    val p = b64url(payload.getBytes(StandardCharsets.UTF_8))
    s"$h.$p."
  }

  /**
   * Writes a driver pod template that uses an init container to place the initial
   * identity token into an emptyDir mounted at /oidc, shared with the driver container.
   * Returns the path to the generated template file.
   *
   * The init container runs as the same user as the Spark driver (uid 185, gid 0 - the
   * `spark` user in the official image) and makes the token file group-writable. This is
   * required because the rotation test later overwrites /oidc/token from inside the
   * driver container (uid 185): if the init container wrote the file as root, the driver
   * would get "Permission denied" and the rotation would silently fail.
   *
   * The init container reuses the Spark image (which already ships `sh`, `printf` and
   * `chmod`) rather than pulling a separate `busybox` image at test time, avoiding an
   * external Docker Hub dependency (and its rate limits / network flakiness) during CI.
   */
  private def writeDriverPodTemplate(initialToken: String): java.nio.file.Path = {
    val tmp = Files.createTempFile("oidc-driver-pod-template-", ".yaml")
    // Write the token, then make it group-writable so the driver (uid 185, gid 0) can
    // overwrite it during the rotation step. Kept as a single value to avoid a long line.
    val initCmd = s"printf '%s' '$initialToken' > /oidc/token && chmod 0660 /oidc/token"
    val yaml =
      s"""apiVersion: v1
         |kind: Pod
         |spec:
         |  volumes:
         |  - name: oidc-token
         |    emptyDir: {}
         |  initContainers:
         |  - name: init-oidc-token
         |    image: $sparkImage
         |    securityContext:
         |      runAsUser: 185
         |      runAsGroup: 0
         |    command: ["sh", "-c", "$initCmd"]
         |    volumeMounts:
         |    - name: oidc-token
         |      mountPath: /oidc
         |  containers:
         |  - name: spark-kubernetes-driver
         |    volumeMounts:
         |    - name: oidc-token
         |      mountPath: /oidc
         |""".stripMargin
    Files.write(tmp, yaml.getBytes(StandardCharsets.UTF_8))
    tmp
  }

  /**
   * Overwrites /oidc/token in the running driver pod with a new token, simulating an
   * external token rotation. Uses the fabric8 client's exec support.
   *
   * The token is embedded in a single-quoted shell argument. makeUnsignedJwt produces
   * only Base64URL characters and '.' separators (no quotes or shell metacharacters),
   * and this method additionally asserts that invariant before executing, so the token
   * cannot break out of the single-quoted argument.
   *
   * Correctness note: we wait on `ExecWatch.exitCode()` (which completes only when the
   * remote command has finished), NOT merely on the ExecListener's onClose callback.
   * onClose fires on WebSocket close and does not guarantee the `printf ... > /oidc/token`
   * redirection has actually completed on the pod; closing the ExecWatch before then can
   * even truncate the write. Waiting for the exit code guarantees the file has been
   * written before this method returns, so the driver's next renewal observes the new
   * token (this was the root cause of the rotation test intermittently reading the stale
   * initial token).
   */
  private def rewriteDriverTokenFile(driverPodName: String, newToken: String): Unit = {
    // Defense in depth: the token is interpolated into a single-quoted shell argument
    // below. makeUnsignedJwt only emits Base64URL characters and '.' separators, but
    // enforce that invariant here so a future change to token generation cannot silently
    // introduce a shell-injection vector (e.g. an embedded single quote or ';').
    require(newToken.matches("[A-Za-z0-9_.-]+"),
      s"Refusing to write a token containing shell-unsafe characters: '$newToken'. " +
        "The token must consist only of Base64URL characters and '.' separators.")
    val exec = kubernetesClient.pods().inNamespace(namespace).withName(driverPodName)
      .inContainer("spark-kubernetes-driver")
      .writingOutput(System.out)
      .writingError(System.err)
      .exec("sh", "-c", s"printf '%s' '$newToken' > /oidc/token")
    try {
      // Block until the remote command actually completes (the write has landed).
      // exitCode() may complete with null if the exec channel closes without ever
      // delivering an exit-code message (e.g. the pod is killed mid-exec); guard the
      // unboxing so that surfaces as a clear assertion failure rather than an NPE.
      val exitCode = exec.exitCode().get(30, java.util.concurrent.TimeUnit.SECONDS)
      assert(exitCode != null && exitCode == 0,
        s"Writing the rotated token to /oidc/token failed with exit code $exitCode.")
    } finally {
      exec.close()
    }
  }

  private def resolveSparkHomeDir(): java.nio.file.Path = {
    // Pick the first candidate directory that actually contains bin/spark-submit.
    // spark.test.home is set to the Spark source root by SparkBuild (sbt) and via the
    // pom's systemProperties (Maven) for this module; user.dir is the last-resort
    // fallback. (Unlike kubernetes-integration-tests, this module does not unpack a
    // distribution, so there is no unpackSparkDir to consult.)
    val candidates = Seq(
      sys.props.get("spark.test.home"),
      sys.props.get("user.dir"))
      .flatten
      // Drop unset/empty/"null" values: Maven forwards empty pom properties as the
      // string "null" (see prop() above), which is not a usable directory.
      .filter(v => v.nonEmpty && v != "null")
    val resolved = candidates.find { dir =>
      new java.io.File(Paths.get(dir).toFile, "bin/spark-submit").exists()
    }.getOrElse {
      throw new IllegalStateException(
        s"Could not find bin/spark-submit under any of: ${candidates.mkString(", ")}. " +
          "Set -Dspark.test.home to a Spark home that contains bin/spark-submit.")
    }
    Paths.get(resolved)
  }

  private def buildS3Client(): S3Client = {
    // moto accepts any non-empty credentials. Use the host-reachable endpoint
    // (loopback), which differs from the pod-facing endpoint used in Spark conf.
    S3Client.builder()
      .endpointOverride(URI.create(s3ClientEndpoint))
      .region(Region.US_EAST_1)
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("test", "test")))
      .forcePathStyle(true)
      .build()
  }

  private def ensureNamespace(): Unit = {
    val existing = kubernetesClient.namespaces().withName(namespace).get()
    if (existing == null) {
      kubernetesClient.namespaces().resource(
        new NamespaceBuilder()
          .withNewMetadata().withName(namespace).endMetadata()
          .build()).create()
      createdNamespace = true
    }
    // Grant RBAC regardless of whether we created the namespace, so the suite also works
    // when pointed at a pre-existing namespace on an RBAC-enabled cluster.
    ensureDriverRbac()
  }

  /**
   * Grants the driver's ServiceAccount permission to manage executor pods within the
   * test namespace, via a namespaced Role + RoleBinding (mirroring the "pods: *" rule in
   * resource-managers/kubernetes/integration-tests/dev/spark-rbac.yaml). Without this,
   * on an RBAC-enabled cluster the default SA cannot create executor pods and the tests
   * time out. Scoping this to the test namespace (rather than relying on a cluster-wide
   * grant) also makes the suite self-sufficient when run locally.
   *
   * Idempotent: re-running against a namespace that already carries these objects
   * (e.g. a fixed, pre-existing namespace) is tolerated -- an already-exists conflict is
   * ignored.
   */
  private def ensureDriverRbac(): Unit = {
    val roleName = "oidc-e2e-driver-role"
    val role = new RoleBuilder()
      .withNewMetadata().withName(roleName).withNamespace(namespace).endMetadata()
      .withRules(new PolicyRuleBuilder()
        .withApiGroups("")
        .withResources("pods", "services", "configmaps", "persistentvolumeclaims")
        .withVerbs("*")
        .build())
      .build()
    createIgnoringConflict {
      kubernetesClient.rbac().roles().inNamespace(namespace).resource(role).create()
    }

    val roleBinding = new RoleBindingBuilder()
      .withNewMetadata().withName("oidc-e2e-driver-role-binding")
      .withNamespace(namespace).endMetadata()
      .withNewRoleRef("rbac.authorization.k8s.io", "Role", roleName)
      .addNewSubject()
        .withKind("ServiceAccount").withName(serviceAccountName).withNamespace(namespace)
        .endSubject()
      .build()
    createIgnoringConflict {
      kubernetesClient.rbac().roleBindings().inNamespace(namespace).resource(roleBinding)
        .create()
    }
  }

  /** Runs a create() call, ignoring a 409 Conflict (resource already exists). */
  private def createIgnoringConflict(create: => Any): Unit = {
    try {
      create
    } catch {
      case e: io.fabric8.kubernetes.client.KubernetesClientException if e.getCode == 409 =>
        logInfo(s"RBAC object already exists; reusing it: ${e.getMessage}")
    }
  }

  /**
   * Ensures the S3 bucket used by the tests exists in moto. RBAC for the driver's
   * ServiceAccount is granted by [[ensureDriverRbac]] when this suite creates the
   * namespace.
   */
  private def ensureMotoResources(): Unit = {
    // Create S3 bucket in moto if it does not exist
    val buckets = s3Client.listBuckets().buckets().asScala.map(_.name())
    if (!buckets.contains(s3Bucket)) {
      s3Client.createBucket(CreateBucketRequest.builder().bucket(s3Bucket).build())
    }
  }
}

private[integrationtest] object OidcCredentialE2ESuite {

  val SPARK_VERSION: String = org.apache.spark.SPARK_VERSION

  val OIDC_JOB_MAIN_CLASS: String =
    "org.apache.spark.security.aws.integrationtest.OidcS3ReadWriteJob"

  val ROTATION_JOB_MAIN_CLASS: String =
    "org.apache.spark.security.aws.integrationtest.OidcTokenRotationJob"

  val LATE_EXECUTOR_JOB_MAIN_CLASS: String =
    "org.apache.spark.security.aws.integrationtest.OidcLateExecutorJob"

  val TIMEOUT: Timeout = Timeout(Span(10, Minutes))
  // After spark-submit returns (it blocks until the driver completes), the output
  // should already be present; only a short poll is needed for eventual consistency.
  val VERIFY_TIMEOUT: Timeout = Timeout(Span(2, Minutes))
  val INTERVAL: Interval = Interval(Span(10, Seconds))

  /** How long to wait for a namespace/pod deletion to complete before giving up. */
  val DELETE_TIMEOUT_MS: Long = 60000L

  /** ScalaTest tag to mark tests that require a running Kubernetes cluster and moto. */
  object oidcE2eTag extends Tag("org.apache.spark.security.aws.integrationtest.OidcE2ETest")
}
