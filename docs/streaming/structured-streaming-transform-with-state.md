---
layout: global
displayTitle: TransformWithState Programming Guide
title: TransformWithState Programming Guide
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

# TransformWithState Programming Guide

The TransformWithState API enables stateful stream processing in Structured Streaming, allowing you to maintain and update state for each unique key in your streaming data.

## Overview

TransformWithState provides functionality to:
* Maintain state variables for each unique grouping key
* Process records that share the same key together
* Schedule timers for future processing
* Control state expiration via TTL (Time To Live)
* Evolve schemas safely over time
* Initialize state from existing sources

## Key Concepts

### State Variables

State variables store data associated with each unique key. There are three types of state variables, each with specific operations:

#### ValueState
Provides operations for single value state:
* `exists()`: Check if state exists
* `get()`: Get the state value if it exists
* `update(newState)`: Update the value
* `clear()`: Remove this state

#### ListState
Provides operations for list state:
* `exists()`: Check if state exists
* `get()`: Get iterator over all values
* `put(newState)`: Set the entire list
* `appendValue(newState)`: Append single value
* `appendList(newState)`: Append array of values
* `clear()`: Remove this state

#### MapState
Provides operations for map state:
* `exists()`: Check if state exists
* `getValue(key)`: Get value for map key
* `containsKey(key)`: Check if key exists
* `updateValue(key, value)`: Update/add key-value pair
* `iterator()`: Get iterator over all key-value pairs
* `keys()`: Get iterator over all keys
* `values()`: Get iterator over all values
* `removeKey(key)`: Remove specific key
* `clear()`: Remove all state

### Configuration

When initializing state variables in `init()`, you can configure:

<table>
<thead><tr><th>Option</th><th>Description</th></tr></thead>
<tr>
  <td>stateName</td>
  <td>Unique identifier for the state variable</td>
</tr>
<tr>
  <td>encoder</td>
  <td>SQL encoder for the state type</td>
</tr>
<tr>
  <td>ttlConfig</td>
  <td>TTL configuration for state expiration. Use <code>TTLConfig(duration)</code> to set expiration time or <code>TTLConfig.NONE</code> to disable TTL</td>
</tr>
</table>

State variables must be:
1. Declared as instance variables
2. Initialized in `init()` method using `getHandle.getValueState()`, `getListState()`, or `getMapState()`
3. Checked for existence using `.exists()` before first read (no default values)

### Timers

Timers allow scheduling callbacks for future execution in either processing-time or event-time.

#### Timer Operations
Available through `getHandle` in StatefulProcessor:
* `registerTimer(expiryTimestampMs)`: Schedule a timer for the current key
* `deleteTimer(expiryTimestampMs)`: Delete a specific timer
* `listTimers()`: Get iterator of all registered timers for current key

#### Timer Values
The `TimerValues` interface provides:
* `getCurrentProcessingTimeInMs()`: Current processing time as epoch milliseconds
    * Constant throughout a streaming query trigger
    * Use for processing-time timers
* `getCurrentWatermarkInMs()`: Current event time watermark
    * Only available when watermark is set
    * Returns 0 in first micro-batch
    * Use for event-time timers

#### Timer Handling
When a timer expires, `handleExpiredTimer` is called with:
* `key`: The grouping key for this timer
* `timerValues`: Current timer values
* `expiredTimerInfo`: Information about the expired timer
    * `getExpiryTimeInMs()`: Get the timer's expiry time

Key timer characteristics:
* Uniquely identified by timestamp within each key
* Cannot be created/deleted in `init()`
* Will fire even without data for that key
* Fire at or after scheduled time, never before
* Use `timerValues` timestamps for fault tolerance
* Can be created/deleted while processing data or handling another timer

### Initial State
When providing initial state via `StatefulProcessorWithInitialState`:
* Available in first micro-batch only
* Must match grouping key schema of input rows
* Processed via `handleInitialState()` method
* Useful for migrating state from existing queries

### Schema Evolution
Supported schema changes between query versions:
* Adding fields (new fields get null defaults)
* Removing fields
* Upcasting fields (e.g., Int to Long)
* Reordering fields

Schema evolution rules:
* Changes validated at query start
* Fields matched by name, not position
* Must be compatible with all previous versions
* Null defaults for all fields

[Example usage...]