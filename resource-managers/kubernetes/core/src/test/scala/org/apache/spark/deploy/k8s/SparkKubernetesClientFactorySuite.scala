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
package org.apache.spark.deploy.k8s

import java.io.File
import java.nio.file.Files

import io.fabric8.kubernetes.client.{Config => Fabric8Config}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.SparkKubernetesClientFactory.ClientType
import org.apache.spark.util.Utils

class SparkKubernetesClientFactorySuite extends SparkFunSuite {

  private val MASTER = "https://kubernetes.example.com:6443"

  private val TEST_KUBECONFIG =
    """apiVersion: v1
      |kind: Config
      |clusters:
      |- cluster:
      |    server: https://context.example.com:6443
      |  name: test-cluster
      |contexts:
      |- context:
      |    cluster: test-cluster
      |    namespace: current-namespace
      |    user: test-user
      |  name: current-context
      |- context:
      |    cluster: test-cluster
      |    namespace: other-namespace
      |    user: test-user
      |  name: other-context
      |current-context: current-context
      |users:
      |- name: test-user
      |  user: {}
      |""".stripMargin

  // Self-signed certificate and matching PKCS#8 key used only as parseable TLS test fixtures.
  private val TEST_CERT =
    """-----BEGIN CERTIFICATE-----
      |MIIDATCCAemgAwIBAgIUOn3kPwWXLKRoN4AslZhR2+ILgdIwDQYJKoZIhvcNAQEL
      |BQAwDzENMAsGA1UEAwwEdGVzdDAgFw0yNjA3MjQyMDQyMTJaGA8yMTI2MDYzMDIw
      |NDIxMlowDzENMAsGA1UEAwwEdGVzdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
      |AQoCggEBAIhKicd7YAzfjzPIcjcmwQYWgaAOgdNtPLUy/ssoDEh8Oqt96mzb+A9a
      |mKHqeaIWfmbu2jgvGncPfaVRtUv8TgXWx6m9NFg3N7DA0b8Pg7N5/SyoXV26Q3x8
      |uUJRYqvTKxxJEqrP9vjev+K0iUq+tfloKfD5hjQ7YV9m0XKWxlCucUIlw5GainqD
      |sprA1QJWkxE5/mUVb3NlY7jXiqoWKpIXqJ25ZYEYSqFGM3ppJZBXj9p9eRhCvCOs
      |+WvkMLwTW1VwC5ikR/J4yt47514CHIi6Lt8KS7YUGoxfnswSYvZZSRub6S2+nkMg
      |9fhMJgrxS5mNu9mn1aRbLzSGtPsVxnUCAwEAAaNTMFEwHQYDVR0OBBYEFKdhMy0X
      |ixs7JDOmBvqe1VhoKlpMMB8GA1UdIwQYMBaAFKdhMy0Xixs7JDOmBvqe1VhoKlpM
      |MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBABdzMHs0FjjNIeak
      |Bb4nJo+zIDLg3r21bnRc2lb7MAvM3DDOVoskdYfjZgPO0c3hcGWv0Olrzm1T6WWJ
      |wCJOhjDet/grbY297RICcjpahfUwLcsAtCSY2gnBvEh6n26uhttavE8HlY7DRovU
      |YdXrJT39DFo89dwYicyIw30csdQxuFi8A6TIwoML3HZtDwR9w+Fnib2AW8eApZWm
      |DNMK4LKuK1uqyAO6gjEQ2KRkc/sXO5auSvlrNnyNb4EiXypP1hSj3XrzgBntQzdO
      |K0z6qRzr150IMNtu8ET8Y17jS/BFRxtSF55jD0vw2hwyZMZt34gdCC/xJnLIWVRK
      |IV1Ubp4=
      |-----END CERTIFICATE-----
      |""".stripMargin

  private val TEST_KEY =
    """-----BEGIN PRIVATE KEY-----
      |MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCISonHe2AM348z
      |yHI3JsEGFoGgDoHTbTy1Mv7LKAxIfDqrfeps2/gPWpih6nmiFn5m7to4Lxp3D32l
      |UbVL/E4F1sepvTRYNzewwNG/D4Ozef0sqF1dukN8fLlCUWKr0yscSRKqz/b43r/i
      |tIlKvrX5aCnw+YY0O2FfZtFylsZQrnFCJcORmop6g7KawNUCVpMROf5lFW9zZWO4
      |14qqFiqSF6iduWWBGEqhRjN6aSWQV4/afXkYQrwjrPlr5DC8E1tVcAuYpEfyeMre
      |O+deAhyIui7fCku2FBqMX57MEmL2WUkbm+ktvp5DIPX4TCYK8UuZjbvZp9WkWy80
      |hrT7FcZ1AgMBAAECggEACumSQotEU4OK3xCLjz2RHZWrvKRXYhWlqLRz5ibyD35d
      |jDbsEC9Em1DGVnXZq1ALFYlw+31PVyfBsvzp0TMFuZNK2p0Flefwk5G8uYQv0qz6
      |cnBAmYKSaLiWdL2OHZ0uHyOn/6Pgbb4YCw99dqNHxWJrs+77/y6fS0OGPZszjGjC
      |oExA5+qLb8hJ7gvyZKhaEjJg7TjK8Uha1eiWh2DMEqw4ptmqtjnaelS0PxGc+rqx
      |54sJ9boouKCkikm4AUwV08yAowVUjEY3KOfG7iFSMBONcax3te7sZeoTsyJPM2xc
      |sKE1ycNpLID23NGyI7buvah/v7Gmogrdrf1Jm28MQQKBgQC8mdKeKpQXzgjfVKxC
      |G6AbTqSFxAIkNufc2eLsWlyxS/zYrH04uUn+BHxQJZBv34n+VCaVfkWf2aBM+Jig
      |VfZ1/Se9NwckcoDPDbjn9kRV+daIOIi7vctW3WeUY6kFTKui1AUWdyvtsPSKprJ9
      |q1aLFHTDNS7IYzxQErpraYezvQKBgQC4/yeaKC6lsExtSxAIDq52uZ4iCTt8cS6E
      |6BcrqhPrIqODU9VrQ+RAX9AKHJYIQOb4vusglHx8p0Na9rbAGGABB2DGnW2LPVqX
      |x5Y6R26pHtKZWmc3D5sZoyOsvk7oOuOJesmbHSllFf0Hi1YfaMGxsUUzb/FPryze
      |5ShiQIEtGQKBgQCzLpFjo6A/XYggZhmiVQyv5O96BtbiASgYMwnc85zM9Ryr3nS5
      |x4/8vdY9bvLi1sYL03c04FrVm7Uoa6bV7dXSE0oGApnOjtrB3I+oEdiqtkqT8OI3
      |PAJL7N3TpTuXxVfrwvNyfJZHpK8wa1949aerSywDqitgRqeui5yBICnFIQKBgHBG
      |s2NVJdJ7aDcgym8JcgsuZnHjxo2lJ4WUUpO2mnYnxLNsyudaAgqr29h3Nvt1YHjx
      |bkSMuezxmuh3ObzmTkXxk2OXoidSqkvZ3ywptFjCEzDtdB0vxINPxtQNgFhjfTsw
      |IXGZxkjKipHsl+1iJ8RPgV9RflRNTd9Nly/iVEPpAoGBAJqrTF9echPnHuKiqXuY
      |nU/SXNTqPzRYj63cOrKHXr06ihMZ9gPGYrz8JciHeC6BQREYHRw0DfyhuGXtKp0M
      |QMzz0A4emLLYCFiohOI/pEZGLsVkaqjZV96NQwmqwLzQ9SUuzJS+MTbY6F2JZ7bH
      |IeODylc6BDAqc6mxYFqRDSGg
      |-----END PRIVATE KEY-----
      |""".stripMargin

  private var savedKubeconfig: Option[String] = None
  private var savedBackoffLimit: Option[String] = None
  private var kubeconfigFile: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    savedKubeconfig = sys.props.get(Fabric8Config.KUBERNETES_KUBECONFIG_FILE)
    savedBackoffLimit =
      sys.props.get(Fabric8Config.KUBERNETES_REQUEST_RETRY_BACKOFFLIMIT_SYSTEM_PROPERTY)
    // Isolate auto-configuration from the developer's real ~/.kube/config by pointing the
    // client library at a kubeconfig file that does not exist unless a test writes it.
    kubeconfigFile = new File(Utils.createTempDir(), "config")
    sys.props(Fabric8Config.KUBERNETES_KUBECONFIG_FILE) = kubeconfigFile.getAbsolutePath
  }

  override def afterEach(): Unit = {
    try {
      restoreProperty(Fabric8Config.KUBERNETES_KUBECONFIG_FILE, savedKubeconfig)
      restoreProperty(
        Fabric8Config.KUBERNETES_REQUEST_RETRY_BACKOFFLIMIT_SYSTEM_PROPERTY, savedBackoffLimit)
    } finally {
      super.afterEach()
    }
  }

  private def restoreProperty(key: String, value: Option[String]): Unit = value match {
    case Some(v) => sys.props(key) = v
    case None => sys.props -= key
  }

  private def writeTempFile(dir: File, name: String, content: String): File = {
    val file = new File(dir, name)
    Files.writeString(file.toPath, content)
    file
  }

  private def buildConfig(
      sparkConf: SparkConf = new SparkConf(false),
      master: String = MASTER,
      namespace: Option[String] = Some("test-namespace"),
      authConfPrefix: String = KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
      clientType: ClientType.Value = ClientType.Submission,
      defaultServiceAccountCaCert: Option[File] = None): Fabric8Config = {
    Utils.tryWithResource(SparkKubernetesClientFactory.createKubernetesClient(
      master, namespace, authConfPrefix, clientType, sparkConf, defaultServiceAccountCaCert)) {
      client => client.getConfiguration
    }
  }

  test("submission client carries master, namespace and API version") {
    val config = buildConfig()
    assert(config.getMasterUrl.stripSuffix("/") === MASTER)
    assert(config.getNamespace === "test-namespace")
    assert(config.getApiVersion === "v1")
  }

  test("client type selects its own request and connection timeouts") {
    val sparkConf = new SparkConf(false)
      .set(SUBMISSION_CLIENT_REQUEST_TIMEOUT, 1234)
      .set(SUBMISSION_CLIENT_CONNECTION_TIMEOUT, 2345)
      .set(DRIVER_CLIENT_REQUEST_TIMEOUT, 3456)
      .set(DRIVER_CLIENT_CONNECTION_TIMEOUT, 4567)
    val submission = buildConfig(sparkConf = sparkConf)
    assert(submission.getRequestTimeout === 1234)
    assert(submission.getConnectionTimeout === 2345)
    val driver = buildConfig(
      sparkConf = sparkConf,
      authConfPrefix = KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
      clientType = ClientType.Driver)
    assert(driver.getRequestTimeout === 3456)
    assert(driver.getConnectionTimeout === 4567)
  }

  gridTest("oauth token is parsed under the auth conf prefix")(Seq(
      KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
      KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
      KUBERNETES_AUTH_CLIENT_MODE_PREFIX)) { prefix =>
    val clientType = if (prefix == KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX) {
      ClientType.Submission
    } else {
      ClientType.Driver
    }
    val sparkConf = new SparkConf(false).set(s"$prefix.$OAUTH_TOKEN_CONF_SUFFIX", "token-value")
    val config =
      buildConfig(sparkConf = sparkConf, authConfPrefix = prefix, clientType = clientType)
    assert(config.getOauthToken === "token-value")
  }

  test("oauth token file content is used as the token") {
    withTempDir { dir =>
      val tokenFile = new File(dir, "token")
      Files.writeString(tokenFile.toPath, "file-token")
      val sparkConf = new SparkConf(false)
        .set(s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$OAUTH_TOKEN_FILE_CONF_SUFFIX",
          tokenFile.getAbsolutePath)
      val config = buildConfig(
        sparkConf = sparkConf,
        authConfPrefix = KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        clientType = ClientType.Driver)
      assert(config.getOauthToken === "file-token")
    }
  }

  test("specifying oauth token and oauth token file together is not allowed") {
    val prefix = KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX
    val sparkConf = new SparkConf(false)
      .set(s"$prefix.$OAUTH_TOKEN_CONF_SUFFIX", "token-value")
      .set(s"$prefix.$OAUTH_TOKEN_FILE_CONF_SUFFIX", "/path/to/token")
    val e = intercept[IllegalArgumentException] {
      buildConfig(sparkConf = sparkConf)
    }
    assert(e.getMessage.contains(s"$prefix.$OAUTH_TOKEN_FILE_CONF_SUFFIX"))
    assert(e.getMessage.contains(s"$prefix.$OAUTH_TOKEN_CONF_SUFFIX"))
  }

  test("client key, client cert and CA cert files are taken from the auth conf") {
    withTempDir { dir =>
      val keyFile = writeTempFile(dir, "client.key", TEST_KEY)
      val certFile = writeTempFile(dir, "client.crt", TEST_CERT)
      val caFile = writeTempFile(dir, "ca.crt", TEST_CERT)
      val prefix = KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX
      val sparkConf = new SparkConf(false)
        .set(s"$prefix.$CLIENT_KEY_FILE_CONF_SUFFIX", keyFile.getAbsolutePath)
        .set(s"$prefix.$CLIENT_CERT_FILE_CONF_SUFFIX", certFile.getAbsolutePath)
        .set(s"$prefix.$CA_CERT_FILE_CONF_SUFFIX", caFile.getAbsolutePath)
      val config = buildConfig(sparkConf = sparkConf)
      assert(config.getClientKeyFile === keyFile.getAbsolutePath)
      assert(config.getClientCertFile === certFile.getAbsolutePath)
      assert(config.getCaCertFile === caFile.getAbsolutePath)
    }
  }

  test("default service account CA cert is used unless caCertFile is set") {
    withTempDir { dir =>
      val caCert = writeTempFile(dir, "sa-ca.crt", TEST_CERT)
      val config = buildConfig(
        authConfPrefix = KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        clientType = ClientType.Driver,
        defaultServiceAccountCaCert = Some(caCert))
      assert(config.getCaCertFile === caCert.getAbsolutePath)

      val overrideCa = writeTempFile(dir, "override-ca.crt", TEST_CERT)
      val sparkConf = new SparkConf(false)
        .set(s"$KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX.$CA_CERT_FILE_CONF_SUFFIX",
          overrideCa.getAbsolutePath)
      val overridden = buildConfig(
        sparkConf = sparkConf,
        authConfPrefix = KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        clientType = ClientType.Driver,
        defaultServiceAccountCaCert = Some(caCert))
      assert(overridden.getCaCertFile === overrideCa.getAbsolutePath)
    }
  }

  test("trust certificates flag is propagated") {
    assert(!buildConfig().isTrustCerts)
    val sparkConf = new SparkConf(false).set(KUBERNETES_TRUST_CERTIFICATES, true)
    assert(buildConfig(sparkConf = sparkConf).isTrustCerts)
  }

  test("request retry backoff limit defaults to 3 when unset") {
    sys.props -= Fabric8Config.KUBERNETES_REQUEST_RETRY_BACKOFFLIMIT_SYSTEM_PROPERTY
    val config = buildConfig()
    assert(sys.props(Fabric8Config.KUBERNETES_REQUEST_RETRY_BACKOFFLIMIT_SYSTEM_PROPERTY) === "3")
    assert(config.getRequestRetryBackoffLimit === 3)
  }

  test("existing request retry backoff limit is preserved") {
    sys.props(Fabric8Config.KUBERNETES_REQUEST_RETRY_BACKOFFLIMIT_SYSTEM_PROPERTY) = "5"
    val config = buildConfig()
    assert(sys.props(Fabric8Config.KUBERNETES_REQUEST_RETRY_BACKOFFLIMIT_SYSTEM_PROPERTY) === "5")
    assert(config.getRequestRetryBackoffLimit === 5)
  }

  test("KUBERNETES_CONTEXT selects the context to auto-configure from") {
    Files.writeString(kubeconfigFile.toPath, TEST_KUBECONFIG)
    val sparkConf = new SparkConf(false).set(KUBERNETES_CONTEXT, "other-context")
    val config = buildConfig(sparkConf = sparkConf, namespace = None)
    assert(config.getNamespace === "other-namespace")
  }

  test("empty KUBERNETES_CONTEXT falls back to the current context") {
    Files.writeString(kubeconfigFile.toPath, TEST_KUBECONFIG)
    val sparkConf = new SparkConf(false).set(KUBERNETES_CONTEXT, "")
    val config = buildConfig(sparkConf = sparkConf, namespace = None)
    assert(config.getNamespace === "current-namespace")
  }

  test("explicit namespace overrides the kubeconfig context namespace") {
    Files.writeString(kubeconfigFile.toPath, TEST_KUBECONFIG)
    val config = buildConfig(namespace = Some("explicit-namespace"))
    assert(config.getNamespace === "explicit-namespace")
  }
}
