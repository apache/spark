---
layout: global
displayTitle: Spark Security
title: Security
---

Spark currently supports authentication via a shared secret. Authentication can be configured to be on via the `spark.authenticate` configuration parameter. This parameter controls whether the Spark communication protocols do authentication using the shared secret. This authentication is a basic handshake to make sure both sides have the same shared secret and are allowed to communicate. If the shared secret is not identical they will not be allowed to communicate. The shared secret is created as follows:

* For Spark on [YARN](running-on-yarn.html) deployments, configuring `spark.authenticate` to `true` will automatically handle generating and distributing the shared secret. Each application will use a unique shared secret.
* For other types of Spark deployments, the Spark parameter `spark.authenticate.secret` should be configured on each of the nodes. This secret will be used by all the Master/Workers and applications.

## Web UI

The Spark UI can be secured by using [javax servlet filters](http://docs.oracle.com/javaee/6/api/javax/servlet/Filter.html) via the `spark.ui.filters` setting
and by using [https/SSL](http://en.wikipedia.org/wiki/HTTPS) via [SSL settings](security.html#ssl-configuration).

### Authentication

A user may want to secure the UI if it has data that other users should not be allowed to see. The javax servlet filter specified by the user can authenticate the user and then once the user is logged in, Spark can compare that user versus the view ACLs to make sure they are authorized to view the UI. The configs `spark.acls.enable`, `spark.ui.view.acls` and `spark.ui.view.acls.groups` control the behavior of the ACLs. Note that the user who started the application always has view access to the UI.  On YARN, the Spark UI uses the standard YARN web application proxy mechanism and will authenticate via any installed Hadoop filters.

Spark also supports modify ACLs to control who has access to modify a running Spark application. This includes things like killing the application or a task. This is controlled by the configs `spark.acls.enable`, `spark.modify.acls` and `spark.modify.acls.groups`. Note that if you are authenticating the web UI, in order to use the kill button on the web UI it might be necessary to add the users in the modify acls to the view acls also. On YARN, the modify acls are passed in and control who has modify access via YARN interfaces.
Spark allows for a set of administrators to be specified in the acls who always have view and modify permissions to all the applications. is controlled by the configs `spark.admin.acls` and `spark.admin.acls.groups`. This is useful on a shared cluster where you might have administrators or support staff who help users debug applications.

## Event Logging

If your applications are using event logging, the directory where the event logs go (`spark.eventLog.dir`) should be manually created and have the proper permissions set on it. If you want those log files secured, the permissions should be set to `drwxrwxrwxt` for that directory. The owner of the directory should be the super user who is running the history server and the group permissions should be restricted to super user group. This will allow all users to write to the directory but will prevent unprivileged users from removing or renaming a file unless they own the file or directory. The event log files will be created by Spark with permissions such that only the user and group have read and write access.

## Encryption

Spark supports SSL for HTTP protocols. SASL encryption is supported for the block transfer service
and the RPC endpoints. Shuffle files can also be encrypted if desired.

### SSL Configuration

Configuration for SSL is organized hierarchically. The user can configure the default SSL settings
which will be used for all the supported communication protocols unless they are overwritten by
protocol-specific settings. This way the user can easily provide the common settings for all the
protocols without disabling the ability to configure each one individually. The common SSL settings
are at `spark.ssl` namespace in Spark configuration. The following table describes the
component-specific configuration namespaces used to override the default settings:

<table class="table">
  <tr>
    <th>Config Namespace</th>
    <th>Component</th>
  </tr>
  <tr>
    <td><code>spark.ssl.fs</code></td>
    <td>File download client (used to download jars and files from HTTPS-enabled servers).</td>
  </tr>
  <tr>
    <td><code>spark.ssl.ui</code></td>
    <td>Spark application Web UI</td>
  </tr>
  <tr>
    <td><code>spark.ssl.standalone</code></td>
    <td>Standalone Master / Worker Web UI</td>
  </tr>
  <tr>
    <td><code>spark.ssl.historyServer</code></td>
    <td>History Server Web UI</td>
  </tr>
</table>

The full breakdown of available SSL options  can be found on the [configuration page](configuration.html).
SSL must be configured on each node and configured for each component involved in communication using the particular protocol.

### YARN mode
The key-store can be prepared on the client side and then distributed and used by the executors as the part of the application. It is possible because the user is able to deploy files before the application is started in YARN by using `spark.yarn.dist.files` or `spark.yarn.dist.archives` configuration settings. The responsibility for encryption of transferring these files is on YARN side and has nothing to do with Spark.

For long-running apps like Spark Streaming apps to be able to write to HDFS, it is possible to pass a principal and keytab to `spark-submit` via the `--principal` and `--keytab` parameters respectively. The keytab passed in will be copied over to the machine running the Application Master via the Hadoop Distributed Cache (securely - if YARN is configured with SSL and HDFS encryption is enabled). The Kerberos login will be periodically renewed using this principal and keytab and the delegation tokens required for HDFS will be generated periodically so the application can continue writing to HDFS.

### Standalone mode
The user needs to provide key-stores and configuration options for master and workers. They have to be set by attaching appropriate Java system properties in `SPARK_MASTER_OPTS` and in `SPARK_WORKER_OPTS` environment variables, or just in `SPARK_DAEMON_JAVA_OPTS`. In this mode, the user may allow the executors to use the SSL settings inherited from the worker which spawned that executor. It can be accomplished by setting `spark.ssl.useNodeLocalConf` to `true`. If that parameter is set, the settings provided by user on the client side, are not used by the executors.

### Preparing the key-stores
Key-stores can be generated by `keytool` program. The reference documentation for this tool is
[here](https://docs.oracle.com/javase/7/docs/technotes/tools/solaris/keytool.html). The most basic
steps to configure the key-stores and the trust-store for the standalone deployment mode is as
follows:

* Generate a keys pair for each node
* Export the public key of the key pair to a file on each node
* Import all exported public keys into a single trust-store
* Distribute the trust-store over the nodes

### Configuring SASL Encryption

SASL encryption is currently supported for the block transfer service when authentication
(`spark.authenticate`) is enabled. To enable SASL encryption for an application, set
`spark.authenticate.enableSaslEncryption` to `true` in the application's configuration.

When using an external shuffle service, it's possible to disable unencrypted connections by setting
`spark.network.sasl.serverAlwaysEncrypt` to `true` in the shuffle service's configuration. If that
option is enabled, applications that are not set up to use SASL encryption will fail to connect to
the shuffle service.

## Configuring Ports for Network Security

Spark makes heavy use of the network, and some environments have strict requirements for using tight
firewall settings.  Below are the primary ports that Spark uses for its communication and how to
configure those ports.

### Standalone mode only

<table class="table">
  <tr>
    <th>From</th><th>To</th><th>Default Port</th><th>Purpose</th><th>Configuration
    Setting</th><th>Notes</th>
  </tr>
  <tr>
    <td>Browser</td>
    <td>Standalone Master</td>
    <td>8080</td>
    <td>Web UI</td>
    <td><code>spark.master.ui.port /<br> SPARK_MASTER_WEBUI_PORT</code></td>
    <td>Jetty-based. Standalone mode only.</td>
  </tr>
  <tr>
    <td>Browser</td>
    <td>Standalone Worker</td>
    <td>8081</td>
    <td>Web UI</td>
    <td><code>spark.worker.ui.port /<br> SPARK_WORKER_WEBUI_PORT</code></td>
    <td>Jetty-based. Standalone mode only.</td>
  </tr>
  <tr>
    <td>Driver /<br> Standalone Worker</td>
    <td>Standalone Master</td>
    <td>7077</td>
    <td>Submit job to cluster /<br> Join cluster</td>
    <td><code>SPARK_MASTER_PORT</code></td>
    <td>Set to "0" to choose a port randomly. Standalone mode only.</td>
  </tr>
  <tr>
    <td>Standalone Master</td>
    <td>Standalone Worker</td>
    <td>(random)</td>
    <td>Schedule executors</td>
    <td><code>SPARK_WORKER_PORT</code></td>
    <td>Set to "0" to choose a port randomly. Standalone mode only.</td>
  </tr>
</table>

### All cluster managers

<table class="table">
  <tr>
    <th>From</th><th>To</th><th>Default Port</th><th>Purpose</th><th>Configuration
    Setting</th><th>Notes</th>
  </tr>
  <tr>
    <td>Browser</td>
    <td>Application</td>
    <td>4040</td>
    <td>Web UI</td>
    <td><code>spark.ui.port</code></td>
    <td>Jetty-based</td>
  </tr>
  <tr>
    <td>Browser</td>
    <td>History Server</td>
    <td>18080</td>
    <td>Web UI</td>
    <td><code>spark.history.ui.port</code></td>
    <td>Jetty-based</td>
  </tr>
  <tr>
    <td>Executor /<br> Standalone Master</td>
    <td>Driver</td>
    <td>(random)</td>
    <td>Connect to application /<br> Notify executor state changes</td>
    <td><code>spark.driver.port</code></td>
    <td>Set to "0" to choose a port randomly.</td>
  </tr>
  <tr>
    <td>Executor / Driver</td>
    <td>Executor / Driver</td>
    <td>(random)</td>
    <td>Block Manager port</td>
    <td><code>spark.blockManager.port</code></td>
    <td>Raw socket via ServerSocketChannel</td>
  </tr>
</table>


See the [configuration page](configuration.html) for more details on the security configuration
parameters, and <a href="{{site.SPARK_GITHUB_URL}}/tree/master/core/src/main/scala/org/apache/spark/SecurityManager.scala">
<code>org.apache.spark.SecurityManager</code></a> for implementation details about security.
