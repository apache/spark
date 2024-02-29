---
layout: global
title: Extending Spark Connect with Custom Functionality
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

Apache Spark provides different ways for users and developers to extend the
system with custom functionality using many different extension points. Due to
the separation of the client from the server, some of the extensions mechanism
are changing.

This can be separated into two main categories: First, extensions that are
purely client side and do not extend the way the Spark server is behaving.
Second, extensions that interact directly with Spark and need to run directly on
the driver.


## Client Extensions

- Client extensions are programs that operate purely on the client surface of
  Apache Spark and do not require any server side changes.


## Server Extensions

One of the biggest benefits of Spark Connect is the ability to extend Apache
Spark with extensions and then use them seamlessly in all clients from any
programming language with relative minimal effort.

Building an extension for Spark Connect is an approach aimed at enhancing the
functionality and flexibility of Apache Spark. Spark Connect operates on three
core primitives: relations, expressions, and commands, each serving a unique
purpose within the data processing framework.

Relations in Spark Connect are fundamental to dataset transformations, acting as
the mechanism through which an optional input dataset is transformed into an
output dataset. Conceptually, relations can be likened to tables within a
database, manipulated to achieve desired outcomes. Their functionality closely
mirrors that of the DataFrame API, providing a familiar and intuitive interface
for data manipulation.

Expressions form another critical component of Spark Connect, functioning as
operations that can be applied to individual columns or a set of columns within
a dataset. These expressions are akin to functions in programming, offering a
level of granularity in data processing that is comparable to the operations
available through the Column API. This allows users to easily extend Spark with
custom expression functionality and make it available to all clients.

Commands stand out within Spark Connect as distinct actions that can be
executed. Unlike relations, which focus on the transformation and nesting of
output data, commands represent singular operations that perform specific tasks
on the data. An example of such a command is a Data Manipulation Language (DML)
command, which enables direct manipulation of the data stored within Spark. This
differentiation highlights the versatility and power of Spark Connect, making it
an essential tool for comprehensive data processing and analysis.

Together, these primitives form the backbone of Spark Connect, offering a robust
framework for extending the capabilities of Apache Spark. Through the strategic
use of relations, expressions, and commands, Spark Connect provides developers
and data scientists with a powerful set of tools for advanced data processing,
analysis, and manipulation.

### Spark Connect Request Processing

- In the proto definition of the Spark Connect API, every request for query execution is
  of type `Plan`. A `Plan` is either a relation or a `Command`.

```protobuf
// A [[Plan]] is the structure that carries the runtime information for the execution from the
// client to the server. A [[Plan]] can either be of the type [[Relation]] which is a reference
// to the underlying logical plan or it can be of the [[Command]] type that is used to execute
// commands on the server.
message Plan {
  oneof op_type {
    Relation root = 1;
    Command command = 2;
  }
}
```

- on the server, the request is parsed and handed over to the `SparkConnectPlanner`
- the role of the planner is to transform the input request into a Spark `LogicalPlan`
  that will be executed

To be able to deal with extensions, every `Relation` and `Command` message carries a
special field that captures the intent for extensions.

```protobuf
message Relation {
  RelationCommon common = 1;
  oneof rel_type {
    // ...
    // This field is used to mark extensions to the protocol. When plugins generate arbitrary
    // relations they can add them here. During the planning the correct resolution is done.
    google.protobuf.Any extension = 998;
  }
  // ...
}

message Command {
  oneof command_type {
    // This field is used to mark extensions to the protocol. When plugins generate arbitrary
    // Commands they can add them here. During the planning the correct resolution is done.
    google.protobuf.Any extension = 999;
  }
  // ...
}
```

The `Any` type provided by the protobuf library is a special wrapper class that lets consumers
of protobuf embed arbitrary message types into a message without knowing the exact type. This
in turn allows developers to add new message types at runtime without having to modify the
message specification.


### Relation Plugin


### Expression Plugin


### Command Plugin