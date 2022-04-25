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
  See the License for the specific language governing permissions s
  limitations under the License.
---

# Cloud Credentials Sharing In Spark

Spark, like any other application, requires credentials in order to access 
cloud services, especially services like S3. These credentials, typically 
session credentials, have an expiry time and need to be renewed in order for
Spark to continue accessing the service.

In larger organizations, the organization may use their own identity service
(say LDAP) and the cloud service provider may hook into the organization's 
identity service before generating new session credentials.

Typically, a new credentials provider implementation has to be added to Spark's
classpath in order for such a system to work.

Since every Spark executor will need these session credentials, each executor will
go thru this authentication and requesting of session credentials, and this can
lead to overloading of the organization's authentication services.

One mechanism to reduce this load is to obtain the credentials once in the driver 
and distribute the credentials to each executor using Spark's internal RPC 
mechanism.

This credentials sharing is implemented to provide just such a mechanism.

To use this, an organization simply provides a `HadoopCloudCredentialsProvider` 
and Spark's `CoarseGrainedSchedulerBackend` will fetch the credentials, set the credentials as a
string value in spark conf and distribute them to the executors. 

In addition, the scheduler backend will schedule a renewal thread which will fetch 
new credentials when the old ones are about to expire and distribute them to the executors.
Implementations of `HadoopCredentialsProvider` are identified by the name of the service.

## Usage
To enable cloud credentials sharing the following configuration parameters need to be set to `true`

 | Configuration | Description | Default |
 | --- | --- | --- |
 |`spark.cloud.credentials.sharing.enabled`| enables/disables cloud credentials sharing globally | false |
 |`spark.security.cloud.credentials.<service_name>.enabled` | enables/disables the credentials provider service with name `<service_name>` |  |
 |`spark.security.cloud.credentials.<service_name>` | actual credentials set in spark conf for the credentials service `<service_name>` |  |

