<!--
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
-->

# Deprecated public APIs in `pyspark.ml.connect`

The public machine learning APIs exposed by `pyspark.ml.connect` have been
**deprecated since Spark 4.0.0**. Use the standard `pyspark.ml` APIs, which support
Spark Connect.

New Spark Connect clients should always exclude these deprecated public APIs from
implementation and API-parity targets. Missing support for them is not a parity
gap. Follow the standard APIs, such as
`pyspark.ml.classification.LogisticRegression`, `pyspark.ml.Pipeline`, and
`pyspark.ml.tuning.CrossValidator`.

This directory also contains internal helpers used by standard `pyspark.ml` over
Spark Connect, including [proto.py](proto.py), [serialize.py](serialize.py), and
[readwrite.py](readwrite.py). These helpers remain relevant when implementing
standard ML support in new clients.
