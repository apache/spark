package org.apache.spark.deploy.k8s.integrationtest.backend.docker

import java.nio.file.Paths

import io.fabric8.kubernetes.client.{Config, DefaultKubernetesClient}
import org.apache.spark.deploy.k8s.integrationtest.ProcessUtils
import org.apache.spark.deploy.k8s.integrationtest.backend.IntegrationTestBackend

private[spark] object DockerForDesktopBackend extends IntegrationTestBackend {

  private val KUBECTL_STARTUP_TIMEOUT_SECONDS = 15

  private var defaultClient: DefaultKubernetesClient = _
  private var initialContext = ""

  private def getCurrentContext: String = {
    val outputs = executeKubectl("config", "current-context")
    assert(outputs.size == 1, "Unexpected amount of output from kubectl config current-context")
    outputs.head
  }

  private def setContext(context: String): Unit = {
    val outputs = executeKubectl("config", "use-context", context)
    assert(outputs.size == 1, "Unexpected amount of output from kubectl config use-context")
    val errors = outputs.filter(_.startsWith("error"))
    assert(errors.size == 0, s"Received errors from kubectl: ${errors.head}")
  }

  override def initialize(): Unit = {
    // Switch context if necessary
    // TODO: If we were using Fabric 8 client 3.1.0 then we could
    // instead just use the overload of autoConfigure() that takes the
    // desired context avoiding the need to interact with kubectl at all
    initialContext = getCurrentContext
    if (!initialContext.equals("docker-for-desktop")) {
      setContext("docker-for-desktop")
    }

    // Auto-configure K8S client from K8S config file
    System.setProperty(Config.KUBERNETES_AUTH_TRYKUBECONFIG_SYSTEM_PROPERTY, "true");
    val userHome = System.getProperty("user.home")
    System.setProperty(Config.KUBERNETES_KUBECONFIG_FILE,
      Option(System.getenv("KUBECONFIG"))
        .getOrElse(Paths.get(userHome, ".kube", "config").toFile.getAbsolutePath))
    val config = Config.autoConfigure()

    defaultClient = new DefaultKubernetesClient(config)
  }

  override def cleanUp(): Unit = {
    super.cleanUp()

    // Reset users kubectl context appropriately if necessary
    if (!initialContext.equals("docker-for-desktop")) {
      setContext(initialContext)
    }
  }

  override def getKubernetesClient: DefaultKubernetesClient = {
    defaultClient
  }

  private def executeKubectl(args: String*): Seq[String] = {
    ProcessUtils.executeProcess(
      Array("kubectl") ++ args, KUBECTL_STARTUP_TIMEOUT_SECONDS, true)
  }

}
