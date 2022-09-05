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

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import io.fabric8.kubernetes.client.Config.KUBERNETES_REQUEST_RETRY_BACKOFFLIMIT_SYSTEM_PROPERTY
import io.fabric8.kubernetes.client.Config.autoConfigure
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory
import io.fabric8.kubernetes.client.utils.Utils.getSystemPropertyOrEnvVar
import okhttp3.Dispatcher
import okhttp3.OkHttpClient

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.util.ThreadUtils

/**
 * Spark-opinionated builder for Kubernetes clients. It uses a prefix plus common suffixes to
 * parse configuration keys, similar to the manner in which Spark's SecurityManager parses SSL
 * options for different components.
 */
private[spark] object SparkKubernetesClientFactory extends Logging {

  def createKubernetesClient(
      master: String,
      namespace: Option[String],
      kubernetesAuthConfPrefix: String,
      clientType: ClientType.Value,
      sparkConf: SparkConf,
      defaultServiceAccountToken: Option[File],
      defaultServiceAccountCaCert: Option[File]): KubernetesClient = {
    val oauthTokenFileConf = s"$kubernetesAuthConfPrefix.$OAUTH_TOKEN_FILE_CONF_SUFFIX"
    val oauthTokenConf = s"$kubernetesAuthConfPrefix.$OAUTH_TOKEN_CONF_SUFFIX"
    val oauthTokenFile = sparkConf.getOption(oauthTokenFileConf)
      .map(new File(_))
      .orElse(defaultServiceAccountToken)
    val oauthTokenValue = sparkConf.getOption(oauthTokenConf)
    KubernetesUtils.requireNandDefined(
      oauthTokenFile,
      oauthTokenValue,
      s"Cannot specify OAuth token through both a file $oauthTokenFileConf and a " +
        s"value $oauthTokenConf.")

    val caCertFile = sparkConf
      .getOption(s"$kubernetesAuthConfPrefix.$CA_CERT_FILE_CONF_SUFFIX")
      .orElse(defaultServiceAccountCaCert.map(_.getAbsolutePath))
    val clientKeyFile = sparkConf
      .getOption(s"$kubernetesAuthConfPrefix.$CLIENT_KEY_FILE_CONF_SUFFIX")
    val clientCertFile = sparkConf
      .getOption(s"$kubernetesAuthConfPrefix.$CLIENT_CERT_FILE_CONF_SUFFIX")
    // TODO(SPARK-37687): clean up direct usage of OkHttpClient, see also:
    // https://github.com/fabric8io/kubernetes-client/issues/3547
    val dispatcher = new Dispatcher(
      ThreadUtils.newDaemonCachedThreadPool("kubernetes-dispatcher"))

    // Allow for specifying a context used to auto-configure from the users K8S config file
    val kubeContext = sparkConf.get(KUBERNETES_CONTEXT).filter(_.nonEmpty)
    logInfo("Auto-configuring K8S client using " +
      kubeContext.map("context " + _).getOrElse("current context") +
      " from users K8S config file")

    // if backoff limit is not set then set it to 3
    if (getSystemPropertyOrEnvVar(KUBERNETES_REQUEST_RETRY_BACKOFFLIMIT_SYSTEM_PROPERTY) == null) {
      System.setProperty(KUBERNETES_REQUEST_RETRY_BACKOFFLIMIT_SYSTEM_PROPERTY, "3")
    }

    // Start from an auto-configured config with the desired context
    // Fabric 8 uses null to indicate that the users current context should be used so if no
    // explicit setting pass null
    val config = new ConfigBuilder(autoConfigure(kubeContext.orNull))
      .withApiVersion("v1")
      .withMasterUrl(master)
      .withRequestTimeout(clientType.requestTimeout(sparkConf))
      .withConnectionTimeout(clientType.connectionTimeout(sparkConf))
      .withTrustCerts(sparkConf.get(KUBERNETES_TRUST_CERTIFICATES))
      .withOption(oauthTokenValue) {
        (token, configBuilder) => configBuilder.withOauthToken(token)
      }.withOption(oauthTokenFile) {
        (file, configBuilder) =>
            configBuilder.withOauthToken(Files.toString(file, Charsets.UTF_8))
      }.withOption(caCertFile) {
        (file, configBuilder) => configBuilder.withCaCertFile(file)
      }.withOption(clientKeyFile) {
        (file, configBuilder) => configBuilder.withClientKeyFile(file)
      }.withOption(clientCertFile) {
        (file, configBuilder) => configBuilder.withClientCertFile(file)
      }.withOption(namespace) {
        (ns, configBuilder) => configBuilder.withNamespace(ns)
      }.build()
    val factoryWithCustomDispatcher = new OkHttpClientFactory() {
      override protected def additionalConfig(builder: OkHttpClient.Builder): Unit = {
        builder.dispatcher(dispatcher)
      }
    }
    logDebug("Kubernetes client config: " +
      new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(config))
    new DefaultKubernetesClient(factoryWithCustomDispatcher.createHttpClient(config), config)
  }

  private implicit class OptionConfigurableConfigBuilder(val configBuilder: ConfigBuilder)
    extends AnyVal {

    def withOption[T]
        (option: Option[T])
        (configurator: ((T, ConfigBuilder) => ConfigBuilder)): ConfigBuilder = {
      option.map { opt =>
        configurator(opt, configBuilder)
      }.getOrElse(configBuilder)
    }
  }

  object ClientType extends Enumeration {
    import scala.language.implicitConversions
    val Driver = Val(DRIVER_CLIENT_REQUEST_TIMEOUT, DRIVER_CLIENT_CONNECTION_TIMEOUT)
    val Submission = Val(SUBMISSION_CLIENT_REQUEST_TIMEOUT, SUBMISSION_CLIENT_CONNECTION_TIMEOUT)

    protected case class Val(
        requestTimeoutEntry: ConfigEntry[Int],
        connectionTimeoutEntry: ConfigEntry[Int])
      extends super.Val {
      def requestTimeout(conf: SparkConf): Int = conf.get(requestTimeoutEntry)
      def connectionTimeout(conf: SparkConf): Int = conf.get(connectionTimeoutEntry)
    }

    implicit def convert(value: Value): Val = value.asInstanceOf[Val]
  }
}
