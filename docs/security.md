(bc1qqeysv50ayq0az93s5frfvtf2fe6rt5tfkdfx2y)---
layout: global
displayTitle: Spark Security
title: Security
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---
* This will become a table of contents (this text will be scraped).
{:toc}

# Spark Security: Things You Need To Know

Security features like authentication are not enabled by default. When deploying a cluster that is open to the internet
or an untrusted network, it's important to secure access to the cluster to prevent unauthorized applications
from running on the cluster.

Spark supports multiple deployments types and each one supports different levels of security. Not
all deployment types will be secure in all environments and none are secure by default. Be
sure to evaluate your environment, what Spark supports, and take the appropriate measure to secure
your Spark deployment.

There are many different types of security concerns. Spark does not necessarily protect against
all things. Listed below are some of the things Spark supports. Also check the deployment
documentation for the type of deployment you are using for deployment specific settings. Anything
not documented, Spark does not support.

# Spark RPC (Communication protocol between Spark processes)

## Authentication

Spark currently supports authentication for RPC channels using a shared secret. Authentication can
be turned on by setting the `spark.authenticate` configuration parameter.

The exact mechanism used to generate and distribute the shared secret is deployment-specific. Unless
specified below, the secret must be defined by setting the `spark.authenticate.secret` config
option. The same secret is shared by all Spark applications and daemons in that case, which limits
the security of these deployments, especially on multi-tenant clusters.

The REST Submission Server supports HTTP `Authorization` header with
a cryptographically signed JSON Web Token via `JWSFilter`.
To enable authorization, Spark Master should have
`spark.master.rest.filters=org.apache.spark.ui.JWSFilter` and
`spark.org.apache.spark.ui.JWSFilter.param.secretKey=BASE64URL-ENCODED-KEY` configurations, and
client should provide HTTP `Authorization` header which contains JSON Web Token signed by
the shared secret key. Please note that this feature requires a Spark distribution built with
`jjwt` profile.

### YARN

For Spark on [YARN](running-on-yarn.html), Spark will automatically handle generating and
distributing the shared secret. Each application will use a unique shared secret. In
the case of YARN, this feature relies on YARN RPC encryption being enabled for the distribution of
secrets to be secure.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.yarn.shuffle.server.recovery.disabled</code></td>
  <td>false</td>
  <td>
    Set to true for applications that have higher security requirements and prefer that their
    secret is not saved in the db. The shuffle data of such applications wll not be recovered after
    the External Shuffle Service restarts.
  </td>
  <td>3.5.0</td>
</tr>
</table>

### Kubernetes

On Kubernetes, Spark will also automatically generate an authentication secret unique to each
application. The secret is propagated to executor pods using environment variables. This means
that any user that can list pods in the namespace where the Spark application is running can
also see their authentication secret. Access control rules should be properly set up by the
Kubernetes admin to ensure that Spark authentication is secure.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.authenticate</code></td>
  <td>false</td>
  <td>Whether Spark authenticates its internal connections.</td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.authenticate.secret</code></td>
  <td>None</td>
  <td>
    The secret key used authentication. See above for when this configuration should be set.
  </td>
  <td>1.0.0</td>
</tr>
</table>

Alternatively, one can mount authentication secrets using files and Kubernetes secrets that
the user mounts into their pods.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.authenticate.secret.file</code></td>
  <td>None</td>
  <td>
    Path pointing to the secret key to use for securing connections. Ensure that the
    contents of the file have been securely generated. This file is loaded on both the driver
    and the executors unless other settings override this (see below).
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.authenticate.secret.driver.file</code></td>
  <td>The value of <code>spark.authenticate.secret.file</code></td>
  <td>
    When specified, overrides the location that the Spark driver reads to load the secret.
    Useful when in client mode, when the location of the secret file may differ in the pod versus
    the node the driver is running in. When this is specified,
    <code>spark.authenticate.secret.executor.file</code> must be specified so that the driver
    and the executors can both use files to load the secret key. Ensure that the contents of the file
    on the driver is identical to the contents of the file on the executors.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.authenticate.secret.executor.file</code></td>
  <td>The value of <code>spark.authenticate.secret.file</code></td>
  <td>
    When specified, overrides the location that the Spark executors read to load the secret.
    Useful in client mode, when the location of the secret file may differ in the pod versus
    the node the driver is running in. When this is specified,
    <code>spark.authenticate.secret.driver.file</code> must be specified so that the driver
    and the executors can both use files to load the secret key. Ensure that the contents of the file
    on the driver is identical to the contents of the file on the executors.
  </td>
  <td>3.0.0</td>
</tr>
</table>

Note that when using files, Spark will not mount these files into the containers for you. It is up
you to ensure that the secret files are deployed securely into your containers and that the driver's
secret file agrees with the executors' secret file.

# Network Encryption

Spark supports two mutually exclusive forms of encryption for RPC connections:

The **preferred method** uses TLS (aka SSL) encryption via Netty's support for SSL. Enabling SSL
requires keys and certificates to be properly configured. SSL is standardized and considered more
secure.

The legacy method is an AES-based encryption mechanism relying on a shared secret. This requires
RPC authentication to also be enabled. This method uses a bespoke protocol and it is recommended
to use SSL instead.

One may prefer to use the SSL based encryption in scenarios where compliance mandates the usage
of specific protocols; or to leverage the security of a more standard encryption library. However,
the AES based encryption is simpler to configure and may be preferred if the only requirement
is that data be encrypted in transit.

If both options are enabled in the configuration, the SSL based RPC encryption takes precedence
and the AES based encryption will not be used (and a warning message will be emitted).

## SSL Encryption (Preferred)

Spark supports SSL based encryption for RPC connections. Please refer to the SSL Configuration
section below to understand how to configure it. The SSL settings are mostly similar across the UI
and RPC, however there are a few additional settings which are specific to the RPC implementation.
The RPC implementation uses Netty under the hood (while the UI uses Jetty), which supports a
different set of options.

Unlike the other SSL settings for the UI, the RPC SSL is *not* automatically enabled if
`spark.ssl.enabled` is set. It must be explicitly enabled, to ensure a safe migration path for users
upgrading Spark versions.

## AES-based Encryption (Legacy)

Spark supports AES-based encryption for RPC connections. For encryption to be enabled, RPC
authentication must also be enabled and properly configured. AES encryption uses the
[Apache Commons Crypto](https://commons.apache.org/proper/commons-crypto/) library, and Spark's
configuration system allows access to that library's configuration for advanced users.

This legacy protocol has two mutually incompatible versions. Version 1 omits applying key derivation function
(KDF) to the key exchange protocol's output, while version 2 applies a KDF to ensure that the derived session
key is uniformly distributed. Version 1 is default for backward compatibility. It is **recommended to use version 2**
for better security properties. The version can be configured by setting `spark.network.crypto.authEngineVersion` to
1 or 2 respectively.

There is also support for SASL-based encryption, although it should be considered deprecated. It
is still required when talking to shuffle services from Spark versions older than 2.2.0.

The following table describes the different options available for configuring this feature.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.network.crypto.enabled</code></td>
  <td>false</td>
  <td>
    Enable AES-based RPC encryption, including the new authentication protocol added in 2.2.0.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.network.crypto.cipher</code></td>
  <td>AES/CTR/NoPadding</td>
  <td>
    Cipher mode to use. Defaults "AES/CTR/NoPadding" for backward compatibility, which is not authenticated. 
    Recommended to use "AES/GCM/NoPadding", which is an authenticated encryption mode.
  </td>
  <td>4.0.0, 3.5.2, 3.4.4</td>
</tr>
<tr>
  <td><code>spark.network.crypto.authEngineVersion</code></td>
  <td>1</td>
  <td>Version of AES-based RPC encryption to use. Valid versions are 1 or 2. Version 2 is recommended.</td>
  <td>4.0.0</td>
</tr>
<tr>
  <td><code>spark.network.crypto.config.*</code></td>
  <td>None</td>
  <td>
    Configuration values for the commons-crypto library, such as which cipher implementations to
    use. The config name should be the name of commons-crypto configuration without the
    <code>commons.crypto</code> prefix.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.network.crypto.saslFallback</code></td>
  <td>true</td>
  <td>
    Whether to fall back to SASL authentication if authentication fails using Spark's internal
    mechanism. This is useful when the application is connecting to old shuffle services that
    do not support the internal Spark authentication protocol. On the shuffle service side,
    disabling this feature will block older clients from authenticating.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.authenticate.enableSaslEncryption</code></td>
  <td>false</td>
  <td>
    Enable SASL-based encrypted communication.
  </td>
  <td>2.2.0</td>
</tr>
<tr>
  <td><code>spark.network.sasl.serverAlwaysEncrypt</code></td>
  <td>false</td>
  <td>
    Disable unencrypted connections for ports using SASL authentication. This will deny connections
    from clients that have authentication enabled, but do not request SASL-based encryption.
  </td>
  <td>1.4.0</td>
</tr>
</table>

# Local Storage Encryption

Spark supports encrypting temporary data written to local disks. This covers shuffle files, shuffle
spills and data blocks stored on disk (for both caching and broadcast variables). It does not cover
encrypting output data generated by applications with APIs such as `saveAsHadoopFile` or
`saveAsTable`. It also may not cover temporary files created explicitly by the user.

The following settings cover enabling encryption for data written to disk:

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.io.encryption.enabled</code></td>
  <td>false</td>
  <td>
    Enable local disk I/O encryption. Currently supported by all modes. It's strongly
    recommended that RPC encryption be enabled when using this feature.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.io.encryption.keySizeBits</code></td>
  <td>128</td>
  <td>
    IO encryption key size in bits. Supported values are 128, 192 and 256.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.io.encryption.keygen.algorithm</code></td>
  <td>HmacSHA1</td>
  <td>
    The algorithm to use when generating the IO encryption key. The supported algorithms are
    described in the KeyGenerator section of the Java Cryptography Architecture Standard Algorithm
    Name Documentation.
  </td>
  <td>2.1.0</td>
</tr>
<tr>
  <td><code>spark.io.encryption.commons.config.*</code></td>
  <td>None</td>
  <td>
    Configuration values for the commons-crypto library, such as which cipher implementations to
    use. The config name should be the name of commons-crypto configuration without the
    <code>commons.crypto</code> prefix.
  </td>
  <td>2.1.0</td>
</tr>
</table>


# Web UI

## Authentication and Authorization

Enabling authentication for the Web UIs is done using [jakarta servlet filters](https://jakarta.ee/specifications/servlet/5.0/apidocs/jakarta/servlet/filter).
You will need a filter that implements the authentication method you want to deploy. Spark does not
provide any built-in authentication filters.

Spark also supports access control to the UI when an authentication filter is present. Each
application can be configured with its own separate access control lists (ACLs). Spark
differentiates between "view" permissions (who is allowed to see the application's UI), and "modify"
permissions (who can do things like kill jobs in a running application).

ACLs can be configured for either users or groups. Configuration entries accept comma-separated
lists as input, meaning multiple users or groups can be given the desired privileges. This can be
used if you run on a shared cluster and have a set of administrators or developers who need to
monitor applications they may not have started themselves. A wildcard (`*`) added to specific ACL
means that all users will have the respective privilege. By default, only the user submitting the
application is added to the ACLs.

Group membership is established by using a configurable group mapping provider. The mapper is
configured using the <code>spark.user.groups.mapping</code> config option, described in the table
below.

The following options control the authentication of Web UIs:

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.ui.allowFramingFrom</code></td>
  <td><code>SAMEORIGIN</code></td>
  <td>Allow framing for a specific named URI via <code>X-Frame-Options</code>. By default, allow only from the same origin.</td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.ui.filters</code></td>
  <td>None</td>
  <td>
    Spark supports HTTP <code>Authorization</code> header with a cryptographically signed
    JSON Web Token via <code>org.apache.spark.ui.JWSFilter</code>. <br />
    See the <a href="configuration.html#spark-ui">Spark UI</a> configuration for how to configure
    filters.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.acls.enable</code></td>
  <td>false</td>
  <td>
    Whether UI ACLs should be enabled. If enabled, this checks to see if the user has access
    permissions to view or modify the application. Note this requires the user to be authenticated,
    so if no authentication filter is installed, this option does not do anything.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.admin.acls</code></td>
  <td>None</td>
  <td>
    Comma-separated list of users that have view and modify access to the Spark application.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.admin.acls.groups</code></td>
  <td>None</td>
  <td>
    Comma-separated list of groups that have view and modify access to the Spark application.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.modify.acls</code></td>
  <td>None</td>
  <td>
    Comma-separated list of users that have modify access to the Spark application.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.modify.acls.groups</code></td>
  <td>None</td>
  <td>
    Comma-separated list of groups that have modify access to the Spark application.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.view.acls</code></td>
  <td>None</td>
  <td>
    Comma-separated list of users that have view access to the Spark application.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.ui.view.acls.groups</code></td>
  <td>None</td>
  <td>
    Comma-separated list of groups that have view access to the Spark application.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.user.groups.mapping</code></td>
  <td><code>org.apache.spark.security.ShellBasedGroupsMappingProvider</code></td>
  <td>
    The list of groups for a user is determined by a group mapping service defined by the trait
    <code>org.apache.spark.security.GroupMappingServiceProvider</code>, which can be configured by
    this property.

    <br />By default, a Unix shell-based implementation is used, which collects this information
    from the host OS.

    <br /><em>Note:</em> This implementation supports only Unix/Linux-based environments.
    Windows environment is currently <b>not</b> supported. However, a new platform/protocol can
    be supported by implementing the trait mentioned above.
  </td>
  <td>2.0.0</td>
</tr>
</table>

On YARN, the view and modify ACLs are provided to the YARN service when submitting applications, and
control who has the respective privileges via YARN interfaces.

## Spark History Server ACLs

Authentication for the SHS Web UI is enabled the same way as for regular applications, using
servlet filters.

To enable authorization in the SHS, a few extra options are used:

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.history.ui.acls.enable</code></td>
  <td>false</td>
  <td>
    Specifies whether ACLs should be checked to authorize users viewing the applications in
    the history server. If enabled, access control checks are performed regardless of what the
    individual applications had set for <code>spark.ui.acls.enable</code>. The application owner
    will always have authorization to view their own application and any users specified via
    <code>spark.ui.view.acls</code> and groups specified via <code>spark.ui.view.acls.groups</code>
    when the application was run will also have authorization to view that application.
    If disabled, no access control checks are made for any application UIs available through
    the history server.
  </td>
  <td>1.0.1</td>
</tr>
<tr>
  <td><code>spark.history.ui.admin.acls</code></td>
  <td>None</td>
  <td>
    Comma separated list of users that have view access to all the Spark applications in history
    server.
  </td>
  <td>2.1.1</td>
</tr>
<tr>
  <td><code>spark.history.ui.admin.acls.groups</code></td>
  <td>None</td>
  <td>
    Comma separated list of groups that have view access to all the Spark applications in history
    server.
  </td>
  <td>2.1.1</td>
</tr>
</table>

The SHS uses the same options to configure the group mapping provider as regular applications.
In this case, the group mapping provider will apply to all UIs server by the SHS, and individual
application configurations will be ignored.

## SSL Configuration

Configuration for SSL is organized hierarchically. The user can configure the default SSL settings
which will be used for all the supported communication protocols unless they are overwritten by
protocol-specific settings. This way the user can easily provide the common settings for all the
protocols without disabling the ability to configure each one individually. Note that all settings 
are inherited this way, *except* for `spark.ssl.rpc.enabled` which must be explicitly set.

The following table describes the SSL configuration namespaces:

<table>
  <thead>
  <tr>
    <th>Config Namespace</th>
    <th>Component</th>
  </tr>
  </thead>
  <tr>
    <td><code>spark.ssl</code></td>
    <td>
      The default SSL configuration. These values will apply to all namespaces below, unless
      explicitly overridden at the namespace level.
    </td>
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
  <tr>
    <td><code>spark.ssl.rpc</code></td>
    <td>Spark RPC communication</td>
  </tr>
</table>

The full breakdown of available SSL options can be found below. The `${ns}` placeholder should be
replaced with one of the above namespaces.

<table>
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Supported Namespaces</th></tr></thead>
  <tr>
    <td><code>${ns}.enabled</code></td>
    <td>false</td>
    <td>Enables SSL. When enabled, <code>${ns}.ssl.protocol</code> is required.</td>
    <td>ui,standalone,historyServer,rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.port</code></td>
    <td>None</td>
    <td>
      The port where the SSL service will listen on.

      <br />The port must be defined within a specific namespace configuration. The default
      namespace is ignored when reading this configuration.

      <br />When not set, the SSL port will be derived from the non-SSL port for the
      same service. A value of "0" will make the service bind to an ephemeral port.
    </td>
    <td>ui,standalone,historyServer</td>
  </tr>
  <tr>
    <td><code>${ns}.enabledAlgorithms</code></td>
    <td>None</td>
    <td>
      A comma-separated list of ciphers. The specified ciphers must be supported by JVM.

      <br />The reference list of protocols can be found in the "JSSE Cipher Suite Names" section
      of the Java security guide. The list for Java 17 can be found at
      <a href="https://docs.oracle.com/en/java/javase/17/docs/specs/security/standard-names.html#jsse-cipher-suite-names">this</a>
      page.

      <br />Note: If not set, the default cipher suite for the JRE will be used.
    </td>
    <td>ui,standalone,historyServer,rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.keyPassword</code></td>
    <td>None</td>
    <td>
      The password to the private key in the key store.
    </td>
    <td>ui,standalone,historyServer,rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.keyStore</code></td>
    <td>None</td>
    <td>
      Path to the key store file. The path can be absolute or relative to the directory in which the
      process is started.
    </td>
    <td>ui,standalone,historyServer,rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.keyStorePassword</code></td>
    <td>None</td>
    <td>Password to the key store.</td>
    <td>ui,standalone,historyServer,rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.keyStoreType</code></td>
    <td>JKS</td>
    <td>The type of the key store.</td>
    <td>ui,standalone,historyServer</td>
  </tr>
  <tr>
    <td><code>${ns}.protocol</code></td>
    <td>None</td>
    <td>
      TLS protocol to use. The protocol must be supported by JVM.

      <br />The reference list of protocols can be found in the "Additional JSSE Standard Names"
      section of the Java security guide. For Java 17, the list can be found at
      <a href="https://docs.oracle.com/en/java/javase/17/docs/specs/security/standard-names.html#additional-jsse-standard-names">this</a>
      page.
    </td>
    <td>ui,standalone,historyServer,rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.needClientAuth</code></td>
    <td>false</td>
    <td>
      Whether to require client authentication.
    </td>
    <td>ui,standalone,historyServer</td>
  </tr>
  <tr>
    <td><code>${ns}.trustStore</code></td>
    <td>None</td>
    <td>
      Path to the trust store file. The path can be absolute or relative to the directory in which
      the process is started.
    </td>
    <td>ui,standalone,historyServer,rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.trustStorePassword</code></td>
    <td>None</td>
    <td>Password for the trust store.</td>
    <td>ui,standalone,historyServer,rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.trustStoreType</code></td>
    <td>JKS</td>
    <td>The type of the trust store.</td>
    <td>ui,standalone,historyServer</td>
  </tr>
  <tr>
    <td><code>${ns}.openSSLEnabled</code></td>
    <td>false</td>
    <td>
      Whether to use OpenSSL for cryptographic operations instead of the JDK SSL provider.
      This setting requires the `certChain` and `privateKey` settings to be set.
      This takes precedence over the `keyStore` and `trustStore` settings if both are specified.
      If the OpenSSL library is not available at runtime, we will fall back to the JDK provider.
    </td>
    <td>rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.privateKey</code></td>
    <td>None</td>
    <td>
      Path to the private key file in PEM format. The path can be absolute or relative to the 
      directory in which the process is started. 
      This setting is required when using the OpenSSL implementation.
    </td>
    <td>rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.privateKeyPassword</code></td>
    <td>None</td>
    <td>
      The password to the above private key file in PEM format.
    </td>
    <td>rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.certChain</code></td>
    <td>None</td>
    <td>
      Path to the certificate chain file in PEM format. The path can be absolute or relative to the 
      directory in which the process is started. 
      This setting is required when using the OpenSSL implementation.
    </td>
    <td>rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.trustStoreReloadingEnabled</code></td>
    <td>false</td>
    <td>
      Whether the trust store should be reloaded periodically.
      This setting is mostly only useful in standalone deployments, not k8s or yarn deployments.
    </td>
    <td>rpc</td>
  </tr>
  <tr>
    <td><code>${ns}.trustStoreReloadIntervalMs</code></td>
    <td>10000</td>
    <td>
      The interval at which the trust store should be reloaded (in milliseconds).
      This setting is mostly only useful in standalone deployments, not k8s or yarn deployments.
    </td>
    <td>rpc</td>
  </tr>
</table>

Spark also supports retrieving `${ns}.keyPassword`, `${ns}.keyStorePassword` and `${ns}.trustStorePassword` from
[Hadoop Credential Providers](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).
User could store password into credential file and make it accessible by different components, like:

```
hadoop credential create spark.ssl.keyPassword -value password \
    -provider jceks://hdfs@nn1.example.com:9001/user/backup/ssl.jceks
```

To configure the location of the credential provider, set the `hadoop.security.credential.provider.path`
config option in the Hadoop configuration used by Spark, like:

```
  <property>
    <name>hadoop.security.credential.provider.path</name>
    <value>jceks://hdfs@nn1.example.com:9001/user/backup/ssl.jceks</value>
  </property>
```

Or via SparkConf "spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs@nn1.example.com:9001/user/backup/ssl.jceks".

## Preparing the key stores

Key stores can be generated by `keytool` program. The reference documentation for this tool for
Java 17 is [here](https://docs.oracle.com/en/java/javase/17/docs/specs/man/keytool.html).
The most basic steps to configure the key stores and the trust store for a Spark Standalone
deployment mode is as follows:

* Generate a key pair for each node
* Export the public key of the key pair to a file on each node
* Import all exported public keys into a single trust store
* Distribute the trust store to the cluster nodes

### YARN mode

To provide a local trust store or key store file to drivers running in cluster mode, they can be
distributed with the application using the `--files` command line argument (or the equivalent
`spark.files` configuration). The files will be placed on the driver's working directory, so the TLS
configuration should just reference the file name with no absolute path.

Distributing local key stores this way may require the files to be staged in HDFS (or other similar
distributed file system used by the cluster), so it's recommended that the underlying file system be
configured with security in mind (e.g. by enabling authentication and wire encryption).

### Standalone mode

The user needs to provide key stores and configuration options for master and workers. They have to
be set by attaching appropriate Java system properties in `SPARK_MASTER_OPTS` and in
`SPARK_WORKER_OPTS` environment variables, or just in `SPARK_DAEMON_JAVA_OPTS`.

The user may allow the executors to use the SSL settings inherited from the worker process. That
can be accomplished by setting `spark.ssl.useNodeLocalConf` to `true`. In that case, the settings
provided by the user on the client side are not used.

## HTTP Security Headers

Apache Spark can be configured to include HTTP headers to aid in preventing Cross Site Scripting
(XSS), Cross-Frame Scripting (XFS), MIME-Sniffing, and also to enforce HTTP Strict Transport
Security.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.ui.xXssProtection</code></td>
  <td><code>1; mode=block</code></td>
  <td>
    Value for HTTP X-XSS-Protection response header. You can choose appropriate value
    from below:
    <ul>
      <li><code>0</code> (Disables XSS filtering)</li>
      <li><code>1</code> (Enables XSS filtering. If a cross-site scripting attack is detected,
        the browser will sanitize the page.)</li>
      <li><code>1; mode=block</code> (Enables XSS filtering. The browser will prevent rendering
        of the page if an attack is detected.)</li>
    </ul>
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.ui.xContentTypeOptions.enabled</code></td>
  <td><code>true</code></td>
  <td>
    When enabled, X-Content-Type-Options HTTP response header will be set to "nosniff".
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.ui.strictTransportSecurity</code></td>
  <td>None</td>
  <td>
    Value for HTTP Strict Transport Security (HSTS) Response Header. You can choose appropriate
    value from below and set <code>expire-time</code> accordingly. This option is only used when
    SSL/TLS is enabled.
    <ul>
      <li><code>max-age=&lt;expire-time&gt;</code></li>
      <li><code>max-age=&lt;expire-time&gt;; includeSubDomains</code></li>
      <li><code>max-age=&lt;expire-time&gt;; preload</code></li>
    </ul>
  </td>
  <td>2.3.0</td>
</tr>
</table>


# Configuring Ports for Network Security

Generally speaking, a Spark cluster and its services are not deployed on the public internet.
They are generally private services, and should only be accessible within the network of the
organization that deploys Spark. Access to the hosts and ports used by Spark services should
be limited to origin hosts that need to access the services.

However, like the REST Submission port, Spark also supports HTTP `Authorization` header
with a cryptographically signed JSON Web Token (JWT) for all UI ports.
To use it, a user needs the Spark distribution built with `jjwt` profile and to configure
`spark.ui.filters=org.apache.spark.ui.JWSFilter` and
`spark.org.apache.spark.ui.JWSFilter.param.secretKey=BASE64URL-ENCODED-KEY`.

Below are the primary ports that Spark uses for its communication and how to
configure those ports.

## Standalone mode only

<table>
  <thead>
  <tr>
    <th>From</th><th>To</th><th>Default Port</th><th>Purpose</th><th>Configuration
    Setting</th><th>Notes</th>
  </tr>
  </thead>
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
    <td>External Service</td>
    <td>Standalone Master</td>
    <td>6066</td>
    <td>Submit job to cluster via REST API</td>
    <td><code>spark.master.rest.port</code></td>
    <td>Use <code>spark.master.rest.enabled</code> to enable/disable this service. Standalone mode only.</td>
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

## All cluster managers

<table>
  <thead>
  <tr>
    <th>From</th><th>To</th><th>Default Port</th><th>Purpose</th><th>Configuration
    Setting</th><th>Notes</th>
  </tr>
  </thead>
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


# Kerberos

Spark supports submitting applications in environments that use Kerberos for authentication.
In most cases, Spark relies on the credentials of the current logged in user when authenticating
to Kerberos-aware services. Such credentials can be obtained by logging in to the configured KDC
with tools like `kinit`.

When talking to Hadoop-based services, Spark needs to obtain delegation tokens so that non-local
processes can authenticate. Spark ships with support for HDFS and other Hadoop file systems, Hive
and HBase.

When using a Hadoop filesystem (such HDFS or WebHDFS), Spark will acquire the relevant tokens
for the service hosting the user's home directory.

An HBase token will be obtained if HBase is in the application's classpath, and the HBase
configuration has Kerberos authentication turned (`hbase.security.authentication=kerberos`).

Similarly, a Hive token will be obtained if Hive is in the classpath, and the configuration includes
URIs for remote metastore services (`hive.metastore.uris` is not empty).

If an application needs to interact with other secure Hadoop filesystems, their URIs need to be
explicitly provided to Spark at launch time. This is done by listing them in the
`spark.kerberos.access.hadoopFileSystems` property, described in the configuration section below.

Spark also supports custom delegation token providers using the Java Services
mechanism (see `java.util.ServiceLoader`). Implementations of
`org.apache.spark.security.HadoopDelegationTokenProvider` can be made available to Spark
by listing their names in the corresponding file in the jar's `META-INF/services` directory.

Delegation token support is currently only supported in YARN and Kubernetes mode. Consult the
deployment-specific page for more information.

The following options provides finer-grained control for this feature:

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.security.credentials.${service}.enabled</code></td>
  <td><code>true</code></td>
  <td>
    Controls whether to obtain credentials for services when security is enabled.
    By default, credentials for all supported services are retrieved when those services are
    configured, but it's possible to disable that behavior if it somehow conflicts with the
    application being run.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.kerberos.access.hadoopFileSystems</code></td>
  <td>(none)</td>
  <td>
    A comma-separated list of secure Hadoop filesystems your Spark application is going to access. For
    example, <code>spark.kerberos.access.hadoopFileSystems=hdfs://nn1.com:8032,hdfs://nn2.com:8032,
    webhdfs://nn3.com:50070</code>. The Spark application must have access to the filesystems listed
    and Kerberos must be properly configured to be able to access them (either in the same realm
    or in a trusted realm). Spark acquires security tokens for each of the filesystems so that
    the Spark application can access those remote Hadoop filesystems.
  </td>
  <td>3.0.0</td>
</tr>
</table>

Users can exclude Kerberos delegation token renewal at resource scheduler. Currently it is only supported
on YARN. The configuration is covered in the [Running Spark on YARN](running-on-yarn.html#yarn-specific-kerberos-configuration) page.

## Long-Running Applications

Long-running applications may run into issues if their run time exceeds the maximum delegation
token lifetime configured in services it needs to access.

This feature is not available everywhere. In particular, it's only implemented
on YARN and Kubernetes (both client and cluster modes).

Spark supports automatically creating new tokens for these applications. There are two ways to
enable this functionality.

### Using a Keytab

By providing Spark with a principal and keytab (e.g. using `spark-submit` with `--principal`
and `--keytab` parameters), the application will maintain a valid Kerberos login that can be
used to retrieve delegation tokens indefinitely.

Note that when using a keytab in cluster mode, it will be copied over to the machine running the
Spark driver. In the case of YARN, this means using HDFS as a staging area for the keytab, so it's
strongly recommended that both YARN and HDFS be secured with encryption, at least.

### Using a ticket cache

By setting `spark.kerberos.renewal.credentials` to `ccache` in Spark's configuration, the local
Kerberos ticket cache will be used for authentication. Spark will keep the ticket renewed during its
renewable life, but after it expires a new ticket needs to be acquired (e.g. by running `kinit`).

It's up to the user to maintain an updated ticket cache that Spark can use.

The location of the ticket cache can be customized by setting the `KRB5CCNAME` environment
variable.

## Secure Interaction with Kubernetes

When talking to Hadoop-based services behind Kerberos, it was noted that Spark needs to obtain delegation tokens
so that non-local processes can authenticate. These delegation tokens in Kubernetes are stored in Secrets that are
shared by the Driver and its Executors. As such, there are three ways of submitting a Kerberos job:

In all cases you must define the environment variable: `HADOOP_CONF_DIR` or
`spark.kubernetes.hadoop.configMapName.`

It also important to note that the KDC needs to be visible from inside the containers.

If a user wishes to use a remote HADOOP_CONF directory, that contains the Hadoop configuration files, this could be
achieved by setting `spark.kubernetes.hadoop.configMapName` to a pre-existing ConfigMap.

1. Submitting with a $kinit that stores a TGT in the Local Ticket Cache:
```bash
/usr/bin/kinit -kt <keytab_file> <username>/<krb5 realm>
/opt/spark/bin/spark-submit \
    --deploy-mode cluster \
    --class org.apache.spark.examples.HdfsTest \
    --master k8s://<KUBERNETES_MASTER_ENDPOINT> \
    --conf spark.executor.instances=1 \
    --conf spark.app.name=spark-hdfs \
    --conf spark.kubernetes.container.image=spark:latest \
    --conf spark.kubernetes.kerberos.krb5.path=/etc/krb5.conf \
    local:///opt/spark/examples/jars/spark-examples_<VERSION>.jar \
    <HDFS_FILE_LOCATION>
```
2. Submitting with a local Keytab and Principal
```bash
/opt/spark/bin/spark-submit \
    --deploy-mode cluster \
    --class org.apache.spark.examples.HdfsTest \
    --master k8s://<KUBERNETES_MASTER_ENDPOINT> \
    --conf spark.executor.instances=1 \
    --conf spark.app.name=spark-hdfs \
    --conf spark.kubernetes.container.image=spark:latest \
    --conf spark.kerberos.keytab=<KEYTAB_FILE> \
    --conf spark.kerberos.principal=<PRINCIPAL> \
    --conf spark.kubernetes.kerberos.krb5.path=/etc/krb5.conf \
    local:///opt/spark/examples/jars/spark-examples_<VERSION>.jar \
    <HDFS_FILE_LOCATION>
```

3. Submitting with pre-populated secrets, that contain the Delegation Token, already existing within the namespace
```bash
/opt/spark/bin/spark-submit \
    --deploy-mode cluster \
    --class org.apache.spark.examples.HdfsTest \
    --master k8s://<KUBERNETES_MASTER_ENDPOINT> \
    --conf spark.executor.instances=1 \
    --conf spark.app.name=spark-hdfs \
    --conf spark.kubernetes.container.image=spark:latest \
    --conf spark.kubernetes.kerberos.tokenSecret.name=<SECRET_TOKEN_NAME> \
    --conf spark.kubernetes.kerberos.tokenSecret.itemKey=<SECRET_ITEM_KEY> \
    --conf spark.kubernetes.kerberos.krb5.path=/etc/krb5.conf \
    local:///opt/spark/examples/jars/spark-examples_<VERSION>.jar \
    <HDFS_FILE_LOCATION>
```

3b. Submitting like in (3) however specifying a pre-created krb5 ConfigMap and pre-created `HADOOP_CONF_DIR` ConfigMap
```bash
/opt/spark/bin/spark-submit \
    --deploy-mode cluster \
    --class org.apache.spark.examples.HdfsTest \
    --master k8s://<KUBERNETES_MASTER_ENDPOINT> \
    --conf spark.executor.instances=1 \
    --conf spark.app.name=spark-hdfs \
    --conf spark.kubernetes.container.image=spark:latest \
    --conf spark.kubernetes.kerberos.tokenSecret.name=<SECRET_TOKEN_NAME> \
    --conf spark.kubernetes.kerberos.tokenSecret.itemKey=<SECRET_ITEM_KEY> \
    --conf spark.kubernetes.hadoop.configMapName=<HCONF_CONFIG_MAP_NAME> \
    --conf spark.kubernetes.kerberos.krb5.configMapName=<KRB_CONFIG_MAP_NAME> \
    local:///opt/spark/examples/jars/spark-examples_<VERSION>.jar \
    <HDFS_FILE_LOCATION>
```
# Event Logging

If your applications are using event logging, the directory where the event logs go
(`spark.eventLog.dir`) should be manually created with proper permissions. To secure the log files,
the directory permissions should be set to `drwxrwxrwxt`. The owner and group of the directory
should correspond to the super user who is running the Spark History Server.

This will allow all users to write to the directory but will prevent unprivileged users from
reading, removing or renaming a file unless they own it. The event log files will be created by
Spark with permissions such that only the user and group have read and write access.

# Persisting driver logs in client mode

If your applications persist driver logs in client mode by enabling `spark.driver.log.persistToDfs.enabled`,
the directory where the driver logs go (`spark.driver.log.dfsDir`) should be manually created with proper
permissions. To secure the log files, the directory permissions should be set to `drwxrwxrwxt`. The owner
and group of the directory should correspond to the super user who is running the Spark History Server.

This will allow all users to write to the directory but will prevent unprivileged users from
reading, removing or renaming a file unless they own it. The driver log files will be created by
Spark with permissions such that only the user and group have read and write access.
