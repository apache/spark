---
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

# Delegation Token Handling In Spark

This document aims to explain and demystify delegation tokens as they are used by Spark, since
this topic is generally a huge source of confusion.


## What are delegation tokens and why use them?

Delegation tokens (DTs from now on) are authentication tokens used by some services to replace
Kerberos service tokens. Many services in the Hadoop ecosystem have support for DTs, since they
have some very desirable advantages over Kerberos tokens:

* No need to distribute Kerberos credentials

In a distributed application, distributing Kerberos credentials is tricky. Not all users have
keytabs, and when they do, it's generally frowned upon to distribute them over the network as
part of application data.

DTs allow for a single place (e.g. the Spark driver) to require Kerberos credentials. That entity
can then distribute the DTs to other parts of the distributed application (e.g. Spark executors),
so they can authenticate to services.

* A single token per service is used for authentication

If Kerberos authentication were used, each client connection to a server would require a trip
to the KDC and generation of a service ticket. In a distributed system, the number of service
tickets can balloon pretty quickly when you think about the number of client processes (e.g. Spark
executors) vs. the number of service processes (e.g. HDFS DataNodes). That generates unnecessary
extra load on the KDC, and may even run into usage limits set up by the KDC admin.

* DTs are only used for authentication

DTs, unlike TGTs, can only be used to authenticate to the specific service for which they were
issued. You cannot use an existing DT to create new DTs or to create DTs for a different service.

So in short, DTs are *not* Kerberos tokens. They are used by many services to replace Kerberos
authentication, or even other forms of authentication, although there is nothing (aside from
maybe implementation details) that ties them to Kerberos or any other authentication mechanism.


## Lifecycle of DTs

DTs, unlike Kerberos tokens, are service-specific. There is no centralized location you contact
to create a DT for a service. So, the first step needed to get a DT is being able to authenticate
to the service in question. In the Hadoop ecosystem, that is generally done using Kerberos.

This requires Kerberos credentials to be available somewhere for the application to use. The user
is generally responsible for providing those credentials, which is most commonly done by logging
in to the KDC (e.g. using "kinit"). That generates a (Kerberos) "token cache" containing a TGT
(ticket granting ticket), which can then be used to request service tickets.

There are other ways of obtaining TGTs, but, ultimately, you need a TGT to bootstrap the process.

Once a TGT is available, the target service's client library can then be used to authenticate
to the service using the Kerberos credentials, and request the creation of a delegation token.
This token can now be sent to other processes and used to authenticate to different daemons
belonging to that service.

And thus the first drawback of DTs becomes apparent: you need service-specific logic to create and
use them.

Spark implements a (somewhat) pluggable, internal DT creation API. Support for new services can be
added by implementing a `HadoopDelegationTokenProvider` that is then called by Spark when generating
delegation tokens for an application. Spark makes the DTs available to code by stashing them in the
`UserGroupInformation` credentials, and it's up to the DT provider and the respective client library
to agree on how to use those tokens.

Once they are created, the semantics of how DTs operate are also service-specific. But, in general,
they try to follow the semantics of Kerberos tokens:

* A "renewable period (equivalent to TGT's "lifetime") which is for how long the DT is valid
  before it requires renewal.
* A "max lifetime" (equivalent to TGT's "renewable life") which is for how long the DT can be
  renewed.

Once the token reaches its "max lifetime", a new one needs to be created by contacting the
appropriate service, restarting the above process.


## DT Renewal, Renewers, and YARN

This is the most confusing part of DT handling, and part of it is because much of the system was
designed with MapReduce, and later YARN, in mind.

As seen above, DTs need to be renewed periodically until they finally expire for good. An example of
this is the default configuration of HDFS services: delegation tokens are valid for up to 7 days,
and need to be renewed every 24 hours. If 24 hours pass without the token being renewed, the token
cannot be used anymore. And the token cannot be renewed anymore after 7 days.

This raises the question: who renews tokens? And for a long time the answer was YARN.

When YARN applications are submitted, a set of DTs is also submitted with them. YARN takes care
of distributing these tokens to containers (using conventions set by the `UserGroupInformation`
API) and, also, keeping them renewed while the app is running. These tokens are used not just
by the application; they are also used by YARN itself to implement features like log collection
and aggregation.

But this has a few caveats.


1. Who renews the tokens?

This is handled mostly transparently by the Hadoop libraries in the case of YARN. Some services have
the concept of a token "renewer". This "renewer" is the name of the principal that is allowed to
renew the DT. When submitting to YARN, that will be the principal that the YARN service is running
as, which means that the client application needs to know that information.

For other resource managers, the renewer mostly does not matter, since there is no service that
is doing the renewal. Except that it sometimes leaks into library code, such as in SPARK-20328.


2. What tokens are renewed?

This is probably the biggest caveat.

As discussed in the previous section, DTs are service-specific, and require service-specific
libraries for creation *and* renewal. This means that for YARN to be able to renew application
tokens, YARN needs:

* The client libraries for all the services the application is using
* Information about how to connect to the services the application is using
* Permissions to connect to those services

In reality, though, most of the time YARN has access to a single HDFS cluster, and that will be
the extent of its DT renewal features. Any other tokens sent to YARN will be distributed to
containers, but will not be renewed.

This means that those tokens will expire way before their max lifetime, unless some other code
takes care of renewing them.

Also, not all client libraries even implement token renewal. To use the example of a service
supported by Spark, the `renew()` method of HBase tokens is a no-op. So the only way to "renew" an
HBase token is to create a new one.


3. What happens when tokens expire for good?

The final caveat is that DTs have a maximum life, regardless of renewal. And after that deadline
is met, you need to create new tokens to be able to connect to the services. That means you need
the ability to connect to the service without a delegation token, which requires some form of
authentication aside from DTs.

This is especially important for long-running applications that run unsupervised. They must be
able to keep on going without having someone logging into a terminal and typing a password every
few days.


## DT Renewal in Spark

Because of the issues explained above, Spark implements a different way of doing renewal. Spark's
solution is a compromise: it targets the lowest common denominator, which is services like HBase
that do not support actual token renewal.

In Spark, DT "renewal" is enabled by giving the application a Kerberos keytab. A keytab is
basically your Kerberos password written into a plain text file, which is why it's so sensitive:
if anyone is able to get hold of that keytab file, they'll be able to authenticate to any service
as that user, for as long as the credentials stored in the keytab remain valid in the KDC.

By having the keytab, Spark can indefinitely maintain a valid Kerberos TGT.

With Kerberos credentials available, Spark will create new DTs for the configured services as old
ones expire. So Spark doesn't renew tokens as explained in the previous section: it will create new
tokens at every renewal interval instead, and distribute those tokens to executors.

This also has another advantage on top of supporting services like HBase: it removes the dependency
on an external renewal service (like YARN). That way, Spark's renewal feature can be used with
resource managers that are not DT-aware, such as Kubernetes, as long as the application
has access to a keytab.


## DTs and Proxy Users

"Proxy users" is Hadoop-speak for impersonation. It allows user A to impersonate user B when
connecting to a service, if that service allows it.

Spark allows impersonation when submitting applications, so that the whole application runs as
user B in the above example.

Spark does not allow token renewal when impersonation is on. Impersonation was added in Spark
as a means for services (like Hive or Oozie) to start Spark applications on behalf of users.
That means that those services would provide the Spark launcher code with privileged credentials
and, potentially, user code that will run when the application starts. The user code is not
necessarily under control of the service.

In that situation, the service credentials should never be made available to the Spark application,
since that would be tantamount to giving your service credentials to unprivileged users.

The above also implies that running impersonated applications in client mode can be a security
concern, since arbitrary user code would have access to the same local content as the privileged
user. But unlike token renewal, Spark does not prevent that configuration from running.

When impersonating, the Spark launcher will create DTs for the "proxy" user. In the example
used above, that means that when code authenticates to a service using the DTs, the authenticated
user will be "B", not "A".

Note that "proxy user" is a very Hadoop-specific concept. It does not apply to OS users (which
is why the client-mode case is an issue) and to services that do not authenticate using Hadoop's
`UserGroupInformation` system. It is generally used in the context of YARN - since an application
submitted as a proxy user will run as that particular user in the YARN cluster, obeying any
Hadoop-to-local-OS-user mapping configured for the service. But the overall support should work
for connecting to other services even when YARN is not being used.

Also, if writing a new DT provider in Spark, be aware that providers need to explicitly handle
impersonation. If a service does not support impersonation, the provider should either error out or
not generate tokens, depending on what makes more sense in the context.


## Externally Generated DTs

Spark uses the `UserGroupInformation` API to manage the Hadoop credentials. That means that Spark
inherits the feature of loading DTs automatically from a file. The Hadoop classes will load the
token cache pointed at by the `HADOOP_TOKEN_FILE_LOCATION` environment variable, when it's defined.

In this situation, Spark will not create DTs for the services that already have tokens in the
cache. It may try to get delegation tokens for other services if Kerberos credentials are also
provided.

This feature is mostly used by services that start Spark on behalf of users. Regular users do not
generally use this feature, given it would require them to figure out how to get those tokens
outside of Spark.


## Limitations of DT support in Spark

There are certain limitations to bear in mind when talking about DTs in Spark.

The first one is that not all DTs actually expose their renewal period. This is generally a
service configuration that is not generally exposed to clients. For this reason, certain DT
providers cannot provide a renewal period to the Spark code, thus requiring that the service's
configuration is in some way synchronized with another one that does provide that information.

The HDFS service, which is generally available when DTs are needed in the first place, provides
that information, so in general it's a good idea for all services using DTs to use the same
configuration as HDFS for the renewal period.

The second one is that Spark doesn't always know what delegation tokens will be needed. For
example, when submitting an application in cluster mode without a keytab, the launcher needs
to create DTs without knowing what the application code will actually be doing. This means that
Spark will try to get as many delegation tokens as is possible based on the configuration
available. That means that if an HBase configuration is available to the launcher but the app
doesn't actually use HBase, a DT will still be generated. The user would have to explicitly
opt out of generating HBase tokens in that case.

The third one is that it's hard to create DTs "as needed". Without being able to authenticate
to specific services, Spark cannot create DTs, which means that applications submitted in cluster
mode like the above need DTs to be created up front, instead of on demand.

The advantage, though, is that user code does not need to worry about DTs, since Spark will handle
them transparently when the proper configuration is available.
