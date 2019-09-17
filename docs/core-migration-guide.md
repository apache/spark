---
layout: global
title: "Migration Guide: Spark Core"
displayTitle: "Migration Guide: Spark Core"
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

* Table of contents
{:toc}

## Upgrading from Core 2.4 to 3.0

- In Spark 3.0, deprecated method `TaskContext.isRunningLocally` has been removed. Local execution was removed and it always has returned `false`.

- In Spark 3.0, deprecated method `shuffleBytesWritten`, `shuffleWriteTime` and `shuffleRecordsWritten` in `ShuffleWriteMetrics` have been removed. Instead, use `bytesWritten`, `writeTime ` and `recordsWritten` respectively.

- In Spark 3.0, deprecated method `AccumulableInfo.apply` have been removed because creating `AccumulableInfo` is disallowed.

