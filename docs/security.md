---
layout: global
title: Spark Security
---

Spark currently supports authentication via a shared secret. Authentication can be configured to be on via the `spark.authenticate` configuration parameter. This parameter controls whether the Spark communication protocols do authentication using the shared secret. This authentication is a basic handshake to make sure both sides have the same shared secret and are allowed to communicate. If the shared secret is not identical they will not be allowed to communicate. The shared secret is created as follows:

* For Spark on [YARN](running-on-yarn.html) deployments, configuring `spark.authenticate` to `true` will automatically handle generating and distributing the shared secret. Each application will use a unique shared secret. 
* For other types of Spark deployments, the Spark parameter `spark.authenticate.secret` should be configured on each of the nodes. This secret will be used by all the Master/Workers and applications.

The Spark UI can also be secured by using [javax servlet filters](http://docs.oracle.com/javaee/6/api/javax/servlet/Filter.html) via the `spark.ui.filters` setting. A user may want to secure the UI if it has data that other users should not be allowed to see. The javax servlet filter specified by the user can authenticate the user and then once the user is logged in, Spark can compare that user versus the view ACLs to make sure they are authorized to view the UI. The configs `spark.ui.acls.enable` and `spark.ui.view.acls` control the behavior of the ACLs. Note that the user who started the application always has view access to the UI.
On YARN, the Spark UI uses the standard YARN web application proxy mechanism and will authenticate via any installed Hadoop filters.

If your applications are using event logging, the directory where the event logs go (`spark.eventLog.dir`) should be manually created and have the proper permissions set on it. If you want those log files secured, the permissions should be set to `drwxrwxrwxt` for that directory. The owner of the directory should be the super user who is running the history server and the group permissions should be restricted to super user group. This will allow all users to write to the directory but will prevent unprivileged users from removing or renaming a file unless they own the file or directory. The event log files will be created by Spark with permissions such that only the user and group have read and write access.

**IMPORTANT NOTE:** *The experimental Netty shuffle path (`spark.shuffle.use.netty`) is not secured, so do not use Netty for shuffles if running with authentication.*

See the [configuration page](configuration.html) for more details on the security configuration parameters.

See <a href="{{site.SPARK_GITHUB_URL}}/tree/master/core/src/main/scala/org/apache/spark/SecurityManager.scala"><code>org.apache.spark.SecurityManager</code></a> for implementation details about security.
