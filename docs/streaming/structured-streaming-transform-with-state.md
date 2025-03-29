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

TransformWithState is the new arbitrary stateful operator in Structured Streaming since the Apache Spark 4.0 release.

This operator has support for an umbrella of features such as object-oriented stateful processor definition, composite types, automatic eviction, timers etc and can be used to build business-critical operational use-cases.

## Defining a Stateful Processor

A stateful processor is defined by extending the StatefulProcessor class and implementing a few methods.

A typical stateful processor deals with the following constructs:
- State Variables - Class specific members used to store user state
- Input Records - Input records received by the stream
- Output Records - Output records produced by the processor. Zero or more output records may be produced by the processor.

### Using State Variables

State variables are class specific members used to store user state. They need to be declared once and initialized within the `init` method of the stateful processor.

For example, consider a stateful processor that counts the number of times a word has been seen in the stream. The state variable `wordCount` is used to store the count of each word. This needs to be declared once and then initialized within the `init` method.

<div data-lang="scala"  markdown="1">

{% highlight scala %}

{% endhighlight %}

</div>

### Types of state variables

State variables can be of the following types:
- Value State
- List State
- Map State

Similar to collections for popular programming languages, the state types could be used to model data structures optimized for various types of operations for the underlying storage layer. For example, appends are optimized for ListState and point lookups are optimized for MapState.
