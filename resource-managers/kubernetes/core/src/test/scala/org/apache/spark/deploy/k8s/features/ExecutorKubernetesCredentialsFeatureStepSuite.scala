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
package org.apache.spark.deploy.k8s.features

import io.fabric8.kubernetes.api.model.{PodBuilder, PodSpec}
import org.apache.logging.log4j.Level
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._

class ExecutorKubernetesCredentialsFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {

  private val EXECUTOR_SA_CONF = KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME.key
  private val DRIVER_SA_CONF = KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME.key
  private val STEP_LOGGER = classOf[ExecutorKubernetesCredentialsFeatureStep].getName

  private var baseConf: SparkConf = _

  before {
    baseConf = new SparkConf(false)
  }

  test("configure spark pod with executor service account") {
    baseConf.set(KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME, "executor-name")
    val spec = evaluateStep()
    assertSAName("executor-name", spec)
  }

  test("configure spark pod with with driver service account " +
    "and without executor service account") {
    baseConf.set(KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME, "driver-name")
    val spec = evaluateStep()
    assertSAName("driver-name", spec)
  }

  test("configure spark pod with with driver service account " +
    "and with executor service account") {
    baseConf.set(KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME, "driver-name")
    baseConf.set(KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME, "executor-name")
    val spec = evaluateStep()
    assertSAName("executor-name", spec)
  }

  test("SPARK-58910: keep the service account named by the executor pod template") {
    // Either spelling means the template already picked an account, so the configured one must not
    // replace it. Varying the configuration alongside the spelling keeps the driver fallback
    // covered here too. The account the step keeps goes into both fields, which is what the API
    // server would store, and when the two disagree the one in `serviceAccountName` counts.
    Seq(
      (EXECUTOR_SA_CONF, podWithAccount(serviceAccountName = Some("template-name"))),
      (DRIVER_SA_CONF, podWithAccount(serviceAccount = Some("template-name"))),
      (EXECUTOR_SA_CONF, podWithAccount(
        serviceAccount = Some("configured-name"), serviceAccountName = Some("template-name")))
    ).foreach { case (conf, templatePod) =>
      val spec = evaluateStep(templatePod, new SparkConf(false).set(conf, "configured-name"))
      withClue(s"via $conf: ") {
        assertSAName("template-name", spec)
      }
    }
  }

  test("SPARK-58910: an empty service account name in the template counts as unset") {
    baseConf.set(KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME, "executor-name")
    // SetDefaults_PodSpec keys off the name being empty rather than null, and an empty alias copies
    // up as an empty name, so neither leaves the pod with an account.
    Seq(
      podWithAccount(serviceAccountName = Some("")),
      podWithAccount(serviceAccount = Some(""))
    ).foreach(templatePod => assertSAName("executor-name", evaluateStep(templatePod)))
  }

  test("SPARK-58910: warn when the template displaces the executor configuration") {
    // Both spellings have to warn, since the account can be named in either field. With both
    // configurations set the message must still name the executor one, because that is the account
    // the step would otherwise have applied.
    Seq(
      Seq(EXECUTOR_SA_CONF -> "configured-name") ->
        podWithAccount(serviceAccountName = Some("template-name")),
      Seq(EXECUTOR_SA_CONF -> "configured-name") ->
        podWithAccount(serviceAccount = Some("template-name")),
      Seq(EXECUTOR_SA_CONF -> "configured-name", DRIVER_SA_CONF -> "driver-name") ->
        podWithAccount(serviceAccountName = Some("template-name")),
      // The deprecated field naming the configured account changes nothing: the step compares
      // against `serviceAccountName`, so this displaces the configuration like the rows above.
      Seq(EXECUTOR_SA_CONF -> "configured-name") ->
        podWithAccount(
          serviceAccount = Some("configured-name"), serviceAccountName = Some("template-name"))
    ).foreach { case (confs, templatePod) =>
      val appender = runWith(confs, templatePod)
      val warnings = messagesAt(appender, Level.WARN)
      assert(warnings.size === 1, s"expected one warning for $confs, got: $warnings")
      // And nothing besides: splitting the report into two independent statements would add a DEBUG
      // line about the driver fallback for anyone who set both configurations.
      assert(appender.loggingEvents.size === 1,
        s"expected the warning to be the only output for $confs: ${allOutput(appender)}")
      // The message names the configuration that did not apply, the account it would have applied,
      // and the account the pod runs as instead.
      Seq(EXECUTOR_SA_CONF, "configured-name", "template-name").foreach { expected =>
        assert(warnings.head.contains(expected),
          s"warning does not name $expected: ${warnings.head}")
      }
      // And names only the configuration that supplied the account: attributing it driver-first,
      // or listing the whole fallback chain, would point the reader at the wrong knob.
      assert(!warnings.head.contains(DRIVER_SA_CONF),
        s"warning names a configuration that did not supply the account: ${warnings.head}")
    }
  }

  test("SPARK-58910: report at DEBUG when the template supersedes the driver's account") {
    // The driver's account is only a fallback for executors and still applies to the driver pod, so
    // a template superseding it is the documented outcome rather than a misconfiguration, and it
    // would otherwise repeat for the life of the application, once per executor pod.
    val appender = runWith(
      Seq(DRIVER_SA_CONF -> "driver-name"),
      podWithAccount(serviceAccountName = Some("template-name")))
    val warnings = messagesAt(appender, Level.WARN)
    assert(warnings.isEmpty, s"a superseded fallback is not worth a warning: $warnings")
    val debugs = messagesAt(appender, Level.DEBUG)
    assert(debugs.size === 1, s"expected one DEBUG line, got: $debugs")
    Seq(DRIVER_SA_CONF, "driver-name", "template-name").foreach { expected =>
      assert(debugs.head.contains(expected), s"DEBUG line does not name $expected: ${debugs.head}")
    }
  }

  test("SPARK-58910: log nothing when nothing the user set was displaced") {
    // Deliberately stricter than "no warning": the step runs once per executor pod, so any output
    // from it in these shapes is noise, down to DEBUG.
    Seq(
      // The template names the account the executor configuration already asked for.
      Seq(EXECUTOR_SA_CONF -> "same-name") ->
        podWithAccount(serviceAccountName = Some("same-name")),
      Seq(EXECUTOR_SA_CONF -> "same-name") -> podWithAccount(serviceAccount = Some("same-name")),
      // Or names it in `serviceAccountName` while the deprecated field names something else:
      // comparing against the wrong field would report a displacement here.
      Seq(EXECUTOR_SA_CONF -> "same-name") ->
        podWithAccount(serviceAccount = Some("other-name"), serviceAccountName = Some("same-name")),
      // The template names no account, so the configured one applies.
      Seq(EXECUTOR_SA_CONF -> "executor-name") -> SparkPod.initialPod(),
      // An empty template field counts as unset, so the configured account applies here too.
      Seq(EXECUTOR_SA_CONF -> "executor-name") -> podWithAccount(serviceAccountName = Some("")),
      Seq(EXECUTOR_SA_CONF -> "executor-name") -> podWithAccount(serviceAccount = Some("")),
      // Nothing is configured, so nothing could have been displaced.
      Seq.empty[(String, String)] -> podWithAccount(serviceAccountName = Some("template-name")),
      // The driver fallback reports at DEBUG, but only when it would have applied a different
      // account, not when the template names the same one.
      Seq(DRIVER_SA_CONF -> "same-name") -> podWithAccount(serviceAccountName = Some("same-name"))
    ).foreach { case (confs, pod) =>
      val output = allOutput(runWith(confs, pod))
      assert(output.isEmpty, s"nothing was displaced with $confs, so nothing to say: $output")
    }
  }

  /**
   * Runs the step under `confs` and hands back the appender that captured the step's output, down
   * to DEBUG so that a test asserting on silence covers that level too.
   */
  private def runWith(confs: Seq[(String, String)], pod: SparkPod): LogAppender = {
    val conf = new SparkConf(false)
    confs.foreach { case (k, v) => conf.set(k, v) }
    val appender = new LogAppender
    // The appender drops anything below INFO on its own, whatever the logger's level.
    appender.setThreshold(Level.DEBUG)
    withLogAppender(appender, loggerNames = Seq(STEP_LOGGER), level = Some(Level.DEBUG)) {
      evaluateStep(pod, conf)
    }
    appender
  }

  private def allOutput(appender: LogAppender): Seq[String] =
    appender.loggingEvents.map(_.getMessage.getFormattedMessage).toSeq

  private def messagesAt(appender: LogAppender, level: Level): Seq[String] =
    appender.loggingEvents.filter(_.getLevel === level).map(_.getMessage.getFormattedMessage).toSeq

  private def assertSAName(expectedServiceAccountName: String,
      spec: PodSpec): Unit = {
    assert(spec.getServiceAccountName.equals(expectedServiceAccountName))
    assert(spec.getServiceAccount.equals(expectedServiceAccountName))
  }

  /**
   * An executor pod whose spec names a service account, standing in for a user pod template. A
   * template is parsed with no API-server defaulting, so it can name either field on its own.
   */
  private def podWithAccount(
      serviceAccount: Option[String] = None,
      serviceAccountName: Option[String] = None): SparkPod = {
    val basePod = SparkPod.initialPod()
    val spec = new PodBuilder(basePod.pod).editOrNewSpec()
    serviceAccount.foreach(spec.withServiceAccount(_))
    serviceAccountName.foreach(spec.withServiceAccountName(_))
    SparkPod(spec.endSpec().build(), basePod.container)
  }

  private def evaluateStep(
      pod: SparkPod = SparkPod.initialPod(),
      conf: SparkConf = baseConf): PodSpec = {
    val executorConf = KubernetesTestConf.createExecutorConf(
        sparkConf = conf)
    val step = new ExecutorKubernetesCredentialsFeatureStep(executorConf)
    step
      .configurePod(pod)
      .pod
      .getSpec
  }
}
