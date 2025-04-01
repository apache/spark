---
layout: global
displayTitle: Structured Streaming Programming Guide
title: Structured Streaming Programming Guide
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

# Overview

TransformWithState is the new arbitrary stateful operator in Structured Streaming since the Apache Spark 4.0 release. This operator is the next generation replacement for the old mapGroupsWithState/flatMapGroupsWithState API for arbitrary stateful processing in Apache Spark.

This operator has support for an umbrella of features such as object-oriented stateful processor definition, composite types, automatic TTL based eviction, timers etc and can be used to build business-critical operational use-cases.

# Language Support

`TransformWithState` is available in Scala, Java and Python. Note that in Python, the operator name is called `transformWithStateInPandas` similar to other operators interacting with the Pandas interface in Apache Spark.

# Components of a TransformWithState Query

A transformWithState query typically consists of the following components:
- Stateful Processor - A user-defined stateful processor that defines the stateful logic
- Output Mode - Output mode for the query such as Append, Update etc
- Time Mode - Time mode for the query such as EventTime, ProcessingTime etc
- Initial State - An optional initial state batch dataframe used to pre-populate the state

In the following sections, we will go through the above components in more detail.

## Defining a Stateful Processor

A stateful processor is the core of the user-defined logic used to operate on the input events. A stateful processor is defined by extending the StatefulProcessor class and implementing a few methods.

A typical stateful processor deals with the following constructs:
- Input Records - Input records received by the stream
- State Variables - Zero or more class specific members used to store user state
- Output Records - Output records produced by the processor. Zero or more output records may be produced by the processor.

A stateful processor uses the object-oriented paradigm to define the stateful logic. The stateful logic is defined by implementing the following methods:
  - `init` - Initialize the stateful processor and define any state variables as needed
  - `handleInputRows` - Process input rows belonging to a grouping key and emit output if needed
  - `handleExpiredTimer` - Handle expired timers and emit output if needed
  - `close` - Perform any cleanup operations if needed
  - `handleInitialState` - Optionally handle the initial state batch dataframe

The methods above will be invoked by the Spark query engine when the operator is executed as part of a streaming query.

### Using the StatefulProcessorHandle

Many operations within the methods above can be performed using the `StatefulProcessorHandle` object. The `StatefulProcessorHandle` object provides methods to interact with the underlying state store. This object can be retrieved within the StatefulProcessor by invoking the `getHandle` method.

### Using State Variables

State variables are class specific members used to store user state. They need to be declared once and initialized within the `init` method of the stateful processor.

Initializing a state variable typically involves the following steps:
- Provide a unique name for the state variable (unique within the stateful processor definition)
- Provide a type for the state variable (ValueState, ListState, MapState) - depending on the type, the appropriate method on the handle needs to be invoked
- Provide a state encoder for the state variable (in Scala - this can be skipped if implicit encoders are available)
- Provide an optional TTL config for the state variable

### Types of state variables

State variables can be of the following types:
- Value State
- List State
- Map State

Similar to collections for popular programming languages, the state types could be used to model data structures optimized for various types of operations for the underlying storage layer. For example, appends are optimized for ListState and point lookups are optimized for MapState.
